from wiki_parser import parse_wiki_page

def read_html_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def main():
    # Read the Abyssal whip page
    html_content = read_html_file('wiki_pages/Abyssal_whip.html')
    
    # Parse and convert to markdown
    markdown = parse_wiki_page(html_content)
    
    # Save the markdown output
    with open('wiki_pages/Abyssal_whip.md', 'w', encoding='utf-8') as f:
        f.write(markdown)
    
    print("Conversion complete! Check Abyssal_whip.md for the result.")

if __name__ == "__main__":
    main() 