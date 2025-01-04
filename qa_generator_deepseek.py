import os
import json
import csv
import time
from pathlib import Path
import concurrent.futures
from typing import List
from deepseek import DeepSeek
from qa_generator import QAGenerator
import multiprocessing
from queue import Empty
from multiprocessing import Queue, Process, Lock

class DeepseekQAGenerator(QAGenerator):
    def __init__(self, markdown_dir, output_dir="qa_dataset_deepseek", process_count=4, threads_per_process=8):
        super().__init__(markdown_dir, output_dir, model="deepseek")
        self.process_count = process_count
        self.threads_per_process = threads_per_process
        self.output_lock = Lock()

    def worker_process(self, file_queue: Queue, output_queue: Queue):
        """Worker process that manages a thread pool for processing files"""
        deepseek = DeepSeek()  # Each process needs its own DeepSeek instance
        
        def process_single_file(md_file):
            try:
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
                
                return md_file, pairs
                
            except Exception as e:
                print(f"Error processing {md_file}: {str(e)}")
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads_per_process) as executor:
            while True:
                try:
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
                    for future in concurrent.futures.as_completed(future_to_file):
                        result = future.result()
                        if result:
                            output_queue.put(result)

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

        while completed_files < total_files:
            try:
                md_file, qa_pairs = output_queue.get(timeout=1)
                if qa_pairs:
                    self.save_qa_pairs_safe(qa_pairs)
                    total_pairs += len(qa_pairs)
                    completed_files += 1
                    print(f"Progress: {completed_files}/{total_files} files | "
                          f"Saved {len(qa_pairs)} pairs from {md_file.name}")
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

        print(f"\nDataset generation complete!")
        print(f"Total Q&A pairs generated: {total_pairs}")
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
    
    args = parser.parse_args()
    
    generator = DeepseekQAGenerator(
        markdown_dir=args.markdown_dir,
        output_dir=args.output_dir,
        process_count=args.processes,
        threads_per_process=args.threads_per_process
    )
    generator.generate_dataset()

if __name__ == "__main__":
    main() 