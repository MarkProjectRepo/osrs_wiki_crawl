import os
import json
import csv
import time
from pathlib import Path
import requests
import argparse

class QAGenerator:
    def __init__(self, markdown_dir, output_dir="qa_dataset", model="mistral"):
        self.markdown_dir = Path(markdown_dir)
        self.output_dir = Path(output_dir)
        self.model = model
        self.output_dir.mkdir(exist_ok=True)
        self.output_file = self.output_dir / 'qa_dataset.csv'
        
        # Create CSV file with header if it doesn't exist
        if not self.output_file.exists():
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['text'])

    def query_ollama(self, prompt, temperature=0.7):
        """Send a query to Ollama API and return the response"""
        try:
            response = requests.post(
                "http://10.0.0.9:11434/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "format": "json",
                    "options": {
                        "temperature": temperature
                    }
                }
            )
            return response.json()["response"]
        except Exception as e:
            print(f"Error querying Ollama: {str(e)}")
            return None

    def assess_document_richness(self, content):
        """Determine number of questions based on document length and content richness"""
        # Count words (more accurate than bytes)
        words = len(content.split())
        
        # Scale based on actual document distribution:
        # - < 50 words (10th percentile): 1 question
        # - 50-160 words (10th-50th): 2 questions
        # - 160-325 words (50th-75th): 3 questions
        # - 325-718 words (75th-90th): 4 questions
        # - > 718 words (>90th): 5 questions
        if words < 50:
            base_questions = 1
        elif words < 160:
            base_questions = 2
        elif words < 325:
            base_questions = 3
        # elif words < 718:
        #     base_questions = 4
        else:
            base_questions = 3
        
        # Add slight randomness (±1 question, but stay within 1-6 range)
        import random
        variation = random.randint(-1, 1)
        num_questions = max(1, min(3, base_questions + variation))
        
        print(f"Word count: {words} -> {num_questions} questions")
        return num_questions

    def generate_qa_pairs(self, content, num_questions):
        """Generate Q&A pairs based on the content"""
        prompt = f"""
        Generate {num_questions} question-answer pairs about the following video game wiki content.
        Return the result in the following JSON format:
        {{
            "qa_pairs": [
                {{
                    "question": "<question text>",
                    "answer": "<answer extracted from the text>"
                }},
                ...
            ]
        }}

        Rules for generation:
        1. Questions should be specific and test knowledge
        2. Answers must be direct quotes or close paraphrases from the text
        3. Generate exactly {num_questions} pairs

        Content:
        {content}
        """
        
        response = self.query_ollama(prompt, temperature=0.7)
        if not response:
            return []
        
        try:
            # Parse JSON response
            result = json.loads(response)
            pairs = []
            for pair in result["qa_pairs"]:
                pairs.append(f"question: {pair['question']}\nanswer: {pair['answer']}")
            return pairs
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {str(e)}")
            print(f"Raw response: {response}")
            return []
        except Exception as e:
            print(f"Error processing QA pairs: {str(e)}")
            print(f"Raw response: {response}")
            return []

    def process_file(self, markdown_file):
        """Process a single markdown file and generate Q&A pairs"""
        try:
            # Read markdown content
            with open(markdown_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Skip if content is too short
            if len(content.strip()) < 100:
                print(f"Skipping {markdown_file.name} - content too short")
                return []
            
            print(f"\nProcessing {markdown_file.name}")
            
            # Assess document richness
            num_questions = self.assess_document_richness(content)
            print(f"Generating {num_questions} questions...")
            
            # Generate Q&A pairs
            qa_pairs = self.generate_qa_pairs(content, num_questions)
            print(f"Generated {len(qa_pairs)} Q&A pairs")
            
            return qa_pairs
            
        except Exception as e:
            print(f"Error processing {markdown_file}: {str(e)}")
            return []

    def save_qa_pairs(self, qa_pairs):
        """Save Q&A pairs to CSV file"""
        with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for pair in qa_pairs:
                writer.writerow([pair])

    def generate_dataset(self):
        """Process all markdown files and generate the dataset"""
        total_pairs = 0
        
        # Get list of markdown files
        markdown_files = list(self.markdown_dir.glob('*.md'))
        print(f"Found {len(markdown_files)} markdown files")
        
        # Process each file
        for md_file in markdown_files:
            qa_pairs = self.process_file(md_file)
            if qa_pairs:
                self.save_qa_pairs(qa_pairs)
                total_pairs += len(qa_pairs)
                print(f"Saved {len(qa_pairs)} pairs to {self.output_file}")
            
            # Add delay to be nice to Ollama
            time.sleep(1)
        
        print(f"\nDataset generation complete!")
        print(f"Total Q&A pairs generated: {total_pairs}")
        print(f"Saved to: {self.output_file}")

def main():
    parser = argparse.ArgumentParser(description='Generate Q&A dataset from markdown files')
    parser.add_argument('--markdown-dir', default='wiki_pages/markdown',
                      help='Directory containing markdown files')
    parser.add_argument('--output-dir', default='qa_dataset',
                      help='Output directory for dataset')
    parser.add_argument('--model', default='mistral',
                      help='Ollama model to use')
    
    args = parser.parse_args()
    
    generator = QAGenerator(
        markdown_dir=args.markdown_dir,
        output_dir=args.output_dir,
        model=args.model
    )
    generator.generate_dataset()

if __name__ == "__main__":
    main()