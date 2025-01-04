import json
from openai import OpenAI
import os


class DeepSeek:
    def __init__(self):
        self.client = OpenAI(
            api_key=os.environ.get("DEEPSEEK_API"),
            base_url="https://api.deepseek.com",
        )

        self.system_prompt = """
            Generate {n_qs} question-answer pairs about the following video game wiki content.
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
            3. Generate exactly {n_qs} pairs

            Content:
            {content}
            """
        

    def generate_qa_pairs(self, content, num_questions):
        messages = [{"role": "system", "content": self.system_prompt.format(n_qs=num_questions, content=content)}]

        response = self.client.chat.completions.create(
            model="deepseek-chat",
            messages=messages,
            response_format={
                'type': 'json_object'
            }
        )

        return json.loads(response.choices[0].message.content)


if __name__ == "__main__":
    with open("/Users/marktraquair/Development/osrs_wiki_crawl/wiki_pages/markdown/Ancient_Magicks.md", "r") as f:
        content = f.read()
    num_questions = 10
    deepseek = DeepSeek()
    print(deepseek.generate_qa_pairs(content, num_questions))
