from bs4 import BeautifulSoup
import re
from typing import Dict, List, Optional, Tuple

class OSRSWikiParser:
    def __init__(self):
        self.infobox_stats = {}
        self.combat_stats = {}
        self.description = ""
        self.sections = {}
        
    def parse_html(self, html_content: str) -> Dict:
        """Parse the HTML content of a wiki page and extract relevant information."""
        soup = BeautifulSoup(html_content, 'html.parser')
        result = {
            'title': self._get_title(soup),
            'infobox': self._parse_infobox(soup),
            'description': self._get_description(soup),
            'sections': self._parse_sections(soup),
            'combat_stats': self._parse_combat_stats(soup),
        }
        return result
    
    def _get_title(self, soup: BeautifulSoup) -> str:
        """Extract the page title."""
        title_elem = soup.find('h1', class_='firstHeading')
        return title_elem.get_text().strip() if title_elem else ""

    def _parse_infobox(self, soup: BeautifulSoup) -> Dict:
        """Parse the infobox information."""
        infobox = soup.find('table', class_='infobox')
        if not infobox:
            return {}
        
        info_dict = {}
        for row in infobox.find_all('tr'):
            header = row.find('th')
            data = row.find('td')
            if header and data:
                key = header.get_text().strip()
                # Handle special cases like images and links
                if data.find('img'):
                    value = [img['src'] for img in data.find_all('img')]
                else:
                    value = data.get_text().strip()
                info_dict[key] = value
                
        return info_dict

    def _get_description(self, soup: BeautifulSoup) -> str:
        """Extract the main description paragraph."""
        content = soup.find('div', class_='mw-parser-output')
        if not content:
            return ""
            
        # Find the first paragraph that's not empty and not a table
        for p in content.find_all('p', recursive=False):
            if p.get_text().strip() and not p.find_parent('table'):
                return self._convert_to_markdown(p)
        return ""

    def _parse_combat_stats(self, soup: BeautifulSoup) -> Dict:
        """Parse combat stats if they exist."""
        stats_table = soup.find('table', class_='infobox-bonuses')
        if not stats_table:
            return {}
            
        stats = {
            'attack_bonuses': {},
            'defence_bonuses': {},
            'other_bonuses': {}
        }
        
        current_section = None
        for row in stats_table.find_all('tr'):
            header = row.find('th', class_='infobox-header')
            if header:
                text = header.get_text().strip().lower()
                if 'attack' in text:
                    current_section = 'attack_bonuses'
                elif 'defence' in text:
                    current_section = 'defence_bonuses'
                elif 'other' in text:
                    current_section = 'other_bonuses'
                continue
                
            if current_section and row.find_all('td', class_='infobox-nested'):
                values = [td.get_text().strip() for td in row.find_all('td', class_='infobox-nested')]
                if current_section == 'attack_bonuses':
                    stats[current_section] = {
                        'stab': values[0],
                        'slash': values[1],
                        'crush': values[2],
                        'magic': values[3],
                        'ranged': values[4]
                    }
                elif current_section == 'defence_bonuses':
                    stats[current_section] = {
                        'stab': values[0],
                        'slash': values[1],
                        'crush': values[2],
                        'magic': values[3],
                        'ranged': values[4]
                    }
                elif current_section == 'other_bonuses':
                    stats[current_section] = {
                        'strength': values[0],
                        'ranged_strength': values[1],
                        'magic_damage': values[2],
                        'prayer': values[3]
                    }
                    
        return stats

    def _parse_sections(self, soup: BeautifulSoup) -> Dict:
        """Parse all content sections."""
        sections = {}
        content = soup.find('div', class_='mw-parser-output')
        if not content:
            return sections
            
        current_section = None
        current_content = []
        
        for elem in content.children:
            if elem.name == 'h2':
                # Save previous section
                if current_section:
                    sections[current_section] = '\n'.join(current_content)
                # Start new section
                current_section = elem.get_text().strip()
                current_content = []
            elif current_section and elem.name in ['p', 'ul', 'table']:
                current_content.append(self._convert_to_markdown(elem))
                
        # Save last section
        if current_section:
            sections[current_section] = '\n'.join(current_content)
            
        return sections

    def _convert_to_markdown(self, elem) -> str:
        """Convert HTML elements to markdown format."""
        if elem.name == 'p':
            text = elem.get_text().strip()
            # Convert links to markdown format
            for link in elem.find_all('a'):
                text = text.replace(str(link), f"[{link.get_text()}]({link.get('href', '')})")
            return text
            
        elif elem.name == 'ul':
            items = []
            for li in elem.find_all('li'):
                items.append(f"* {li.get_text().strip()}")
            return '\n'.join(items)
            
        elif elem.name == 'table':
            # Skip certain table classes that don't need conversion
            if any(c in elem.get('class', []) for c in ['navbox', 'infobox-smw-data']):
                return ""
            
            # Improved table conversion
            rows = []
            headers = []
            data_rows = []
            
            # First pass: collect all headers and determine column count
            max_cols = 0
            for tr in elem.find_all('tr'):
                # Count actual data cells (ignore nested tables/navboxes)
                cells = [c for c in tr.find_all(['th', 'td']) if not c.find_parent('table', class_=['navbox', 'infobox-smw-data'])]
                cols = sum(int(c.get('colspan', 1)) for c in cells)
                max_cols = max(max_cols, cols)
                
                # Collect headers from first row with th elements
                if not headers:
                    header_cells = [c for c in tr.find_all('th') if not c.find_parent('table', class_=['navbox', 'infobox-smw-data'])]
                    if header_cells:
                        headers = []
                        for cell in header_cells:
                            text = cell.get_text().strip()
                            # Remove edit links and other unwanted text
                            text = re.sub(r'\[edit.*?\]', '', text)
                            text = re.sub(r'\s+', ' ', text)
                            headers.append(text)
            
            # Second pass: collect data rows
            for tr in elem.find_all('tr'):
                # Skip pure header rows
                if tr.find_all('th') and tr == elem.find('tr'):
                    continue
                
                cells = []
                # Get all cells (th or td)
                for cell in tr.find_all(['th', 'td']):
                    if cell.find_parent('table') != elem:
                        continue
                        
                    # Clean up cell content
                    content = cell.get_text().strip()
                    # Remove edit links and other unwanted text
                    content = re.sub(r'\[edit.*?\]', '', content)
                    content = re.sub(r'\s+', ' ', content)
                    
                    # Handle colspan
                    colspan = int(cell.get('colspan', 1))
                    if colspan > 1:
                        cells.extend([content] + [''] * (colspan - 1))
                    else:
                        cells.append(content)
                
                # Only add row if it has content
                if cells and any(cell.strip() for cell in cells):
                    # Pad cells if needed
                    while len(cells) < max_cols:
                        cells.append('')
                    data_rows.append(cells)
            
            # If we have data but no headers, generate them
            if data_rows and not headers:
                headers = [f"Column {i+1}" for i in range(max_cols)]
            elif len(headers) < max_cols:
                headers.extend([f"Column {i+1}" for i in range(len(headers), max_cols)])
            
            # Build the markdown table
            if headers and data_rows:
                # Add header row
                rows.append('| ' + ' | '.join(headers) + ' |')
                # Add separator row
                rows.append('|' + '|'.join(['---' for _ in range(max_cols)]) + '|')
                # Add data rows
                for cells in data_rows:
                    rows.append('| ' + ' | '.join(cells) + ' |')
                
                return '\n'.join(rows)
            
            return ""
            
        return ""

    def to_markdown(self, parsed_data: Dict) -> str:
        """Convert parsed data to markdown format."""
        md_parts = []
        
        # Title
        md_parts.append(f"# {parsed_data['title']}\n")
        
        # Description
        if parsed_data['description']:
            md_parts.append(parsed_data['description'] + "\n")
        
        # Infobox
        if parsed_data['infobox']:
            md_parts.append("## Item Information\n")
            for key, value in parsed_data['infobox'].items():
                if isinstance(value, list):  # Handle image lists
                    continue  # Skip images for now
                md_parts.append(f"**{key}:** {value}")
            md_parts.append("")
        
        # Combat stats
        if parsed_data['combat_stats']:
            md_parts.append("## Combat Statistics\n")
            for category, stats in parsed_data['combat_stats'].items():
                md_parts.append(f"### {category.replace('_', ' ').title()}")
                for stat, value in stats.items():
                    md_parts.append(f"* {stat.replace('_', ' ').title()}: {value}")
                md_parts.append("")
        
        # Other sections
        for section, content in parsed_data['sections'].items():
            if section.lower() not in ['combat stats', 'item information']:
                md_parts.append(f"## {section}\n")
                md_parts.append(content + "\n")
        
        return '\n'.join(md_parts)

def parse_wiki_page(html_content: str) -> str:
    """Parse a wiki page and return markdown content."""
    parser = OSRSWikiParser()
    parsed_data = parser.parse_html(html_content)
    return parser.to_markdown(parsed_data) 