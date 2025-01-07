import os
import time
import concurrent.futures
from deepseek import DeepSeek
from qa_generator import QAGenerator
from queue import Empty
from multiprocessing import Queue, Process, Lock
from statistics import mean

class DeepseekQAGenerator(QAGenerator):
    def __init__(self, markdown_dir, output_dir="qa_dataset_deepseek", process_count=4, threads_per_process=8):
        super().__init__(markdown_dir, output_dir, model="deepseek")
        self.process_count = process_count
        self.threads_per_process = threads_per_process
        self.output_lock = Lock()
        self.batch_times = []
        self.file_times = []

    def worker_process(self, file_queue: Queue, output_queue: Queue):
        """Worker process that manages a thread pool for processing files"""
        deepseek = DeepSeek()  # Each process needs its own DeepSeek instance
        
        def process_single_file(md_file):
            try:
                start_time = time.time()
                with open(md_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if len(content.strip()) < 100:
                    print(f"Skipping {md_file.name} - content too short")
                    return None
                
                num_questions = self.assess_document_richness(content)
                result = deepseek.generate_qa_pairs(content, num_questions)
                
                pairs = []
                for pair in result["qa_pairs"]:
                    pairs.append(f"question: {pair['question']}\nanswer: {pair['answer']}")
                
                processing_time = time.time() - start_time
                return md_file, pairs, processing_time
                
            except Exception as e:
                print(f"Error processing {md_file}: {str(e)}")
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads_per_process) as executor:
            while True:
                try:
                    batch_start_time = time.time()
                    # Get a batch of files to process
                    files = []
                    for _ in range(self.threads_per_process):
                        try:
                            files.append(file_queue.get_nowait())
                        except Empty:
                            break
                    
                    if not files:
                        break

                    # Submit files to thread pool
                    future_to_file = {
                        executor.submit(process_single_file, md_file): md_file 
                        for md_file in files
                    }

                    # Process results as they complete
                    batch_results = []
                    for future in concurrent.futures.as_completed(future_to_file):
                        result = future.result()
                        if result:
                            batch_results.append(result)
                            output_queue.put(result)

                    if batch_results:
                        batch_time = time.time() - batch_start_time
                        output_queue.put(("BATCH_TIME", batch_time, len(batch_results)))

                except Empty:
                    break
                except Exception as e:
                    print(f"Error in worker process: {str(e)}")

    def save_qa_pairs_safe(self, qa_pairs):
        """Thread-safe method to save Q&A pairs"""
        with self.output_lock:
            self.save_qa_pairs(qa_pairs)

    def generate_dataset(self):
        """Process all markdown files using multiple processes and threads"""
        markdown_files = list(self.markdown_dir.glob('*.md'))
        print(f"Found {len(markdown_files)} markdown files")

        # Create queues for input files and output results
        file_queue = Queue()
        output_queue = Queue()

        # Put all files in the queue
        for md_file in markdown_files:
            file_queue.put(md_file)

        # Start worker processes
        processes = []
        for _ in range(self.process_count):
            p = Process(target=self.worker_process, args=(file_queue, output_queue))
            p.start()
            processes.append(p)

        # Track progress and save results
        total_pairs = 0
        completed_files = 0
        total_files = len(markdown_files)
        start_time = time.time()

        while completed_files < total_files:
            try:
                result = output_queue.get(timeout=1)
                
                if isinstance(result[0], str) and result[0] == "BATCH_TIME":
                    _, batch_time, batch_size = result
                    self.batch_times.append((batch_time, batch_size))
                    avg_time_per_file = batch_time / batch_size
                    print(f"Batch completed: {batch_size} files in {batch_time:.2f}s "
                          f"(avg: {avg_time_per_file:.2f}s per file)")
                else:
                    md_file, qa_pairs, processing_time = result
                    if qa_pairs:
                        self.save_qa_pairs_safe(qa_pairs)
                        total_pairs += len(qa_pairs)
                        completed_files += 1
                        self.file_times.append(processing_time)
                        print(f"Progress: {completed_files}/{total_files} files | "
                              f"Saved {len(qa_pairs)} pairs from {md_file.name} "
                              f"({processing_time:.2f}s)")
            except Empty:
                # Check if all processes are done
                if all(not p.is_alive() for p in processes):
                    break
            except Exception as e:
                print(f"Error processing results: {str(e)}")
                completed_files += 1

        # Wait for all processes to complete
        for p in processes:
            p.join()

        total_time = time.time() - start_time
        
        # Print timing statistics
        print(f"\nDataset generation complete!")
        print(f"Total time: {total_time:.2f}s")
        print(f"Total Q&A pairs generated: {total_pairs}")
        if self.file_times:
            print(f"Average time per file: {mean(self.file_times):.2f}s")
        if self.batch_times:
            batch_times, batch_sizes = zip(*self.batch_times)
            print(f"Average batch processing time: {mean(batch_times):.2f}s")
            print(f"Average batch size: {mean(batch_sizes):.1f} files")
        print(f"Saved to: {self.output_file}")

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Generate Q&A dataset from markdown files using DeepSeek')
    parser.add_argument('--markdown-dir', default='wiki_pages/markdown',
                      help='Directory containing markdown files')
    parser.add_argument('--output-dir', default='qa_dataset_deepseek',
                      help='Output directory for dataset')
    parser.add_argument('--processes', type=int, default=4,
                      help='Number of worker processes')
    parser.add_argument('--threads-per-process', type=int, default=8,
                      help='Number of threads per process')
    parser.add_argument('--env', default='deepseek.env',
                      help='Path to the environment file')
    
    args = parser.parse_args()
    with open(args.env, 'r') as f:
        api_key = f.read().strip()
    os.environ['DEEPSEEK_API'] = api_key
    generator = DeepseekQAGenerator(
        markdown_dir=args.markdown_dir,
        output_dir=args.output_dir,
        process_count=args.processes,
        threads_per_process=args.threads_per_process
    )
    generator.generate_dataset()

if __name__ == "__main__":
    main() 