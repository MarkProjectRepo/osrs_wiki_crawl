from bs4 import BeautifulSoup
import re

def clean_text(text):
    """Clean up text by removing extra whitespace and newlines"""
    return re.sub(r'\s+', ' ', text).strip()

def parse_infobox(infobox):
    """Parse the item infobox for key properties"""
    info = {}
    
    # Get item name from header
    header = infobox.find('th', class_='infobox-header')
    if header:
        info['name'] = header.get_text(strip=True)
    
    # Get key item properties
    rows = infobox.find_all('tr')
    for row in rows:
        header = row.find('th')
        value = row.find('td')
        if header and value:
            key = header.get_text(strip=True)
            val = clean_text(value.get_text())
            if val:  # Only add non-empty values
                info[key] = val
                
    return info

def parse_combat_stats(soup):
    """Parse combat stats tables"""
    stats = {}
    
    # Find combat stats table
    combat_table = soup.find('table', class_='wikitable combat-styles')
    if combat_table:
        stats['combat_styles'] = []
        rows = combat_table.find_all('tr')[1:]  # Skip header
        for row in rows:
            cols = row.find_all(['td'])
            if len(cols) >= 7:
                style = {
                    'name': clean_text(cols[1].get_text()),
                    'attack_type': clean_text(cols[2].get_text()),
                    'style': clean_text(cols[3].get_text()),
                    'speed': clean_text(cols[4].get_text()),
                }
                stats['combat_styles'].append(style)
    
    return stats

def parse_bonuses(soup):
    """Parse equipment bonuses"""
    bonuses = {}
    
    bonus_table = soup.find('table', class_='infobox-bonuses')
    if bonus_table:
        # Parse attack bonuses
        attack_cells = bonus_table.find_all('td', class_='infobox-nested')[:5]
        bonuses['attack'] = {
            'stab': attack_cells[0].get_text(strip=True),
            'slash': attack_cells[1].get_text(strip=True), 
            'crush': attack_cells[2].get_text(strip=True),
            'magic': attack_cells[3].get_text(strip=True),
            'range': attack_cells[4].get_text(strip=True)
        }
        
        # Parse defense bonuses
        defense_cells = bonus_table.find_all('td', class_='infobox-nested')[5:10]
        bonuses['defense'] = {
            'stab': defense_cells[0].get_text(strip=True),
            'slash': defense_cells[1].get_text(strip=True),
            'crush': defense_cells[2].get_text(strip=True), 
            'magic': defense_cells[3].get_text(strip=True),
            'range': defense_cells[4].get_text(strip=True)
        }
        
        # Parse other bonuses
        other_cells = bonus_table.find_all('td', class_='infobox-nested')[10:14]
        bonuses['other'] = {
            'strength': other_cells[0].get_text(strip=True),
            'ranged_strength': other_cells[1].get_text(strip=True),
            'magic_damage': other_cells[2].get_text(strip=True),
            'prayer': other_cells[3].get_text(strip=True)
        }
        
    return bonuses

def parse_description(soup):
    """Parse main item description"""
    content = soup.find(id="mw-content-text")
    if not content:
        return ""
        
    # Get first few paragraphs of description
    description = []
    for p in content.find_all('p', recursive=False)[:3]:
        text = clean_text(p.get_text())
        if text:
            description.append(text)
            
    return "\n\n".join(description)

def html_to_markdown(html_content):
    """Convert wiki HTML to markdown"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Get item info
    infobox = soup.find('table', class_='infobox')
    item_info = parse_infobox(infobox) if infobox else {}
    
    # Get combat stats
    combat_stats = parse_combat_stats(soup)
    
    # Get equipment bonuses
    bonuses = parse_bonuses(soup)
    
    # Get description
    description = parse_description(soup)
    
    # Build markdown output
    markdown = []
    
    # Add title
    if 'name' in item_info:
        markdown.append(f"# {item_info['name']}\n")
    
    # Add description
    if description:
        markdown.append(description + "\n")
    
    # Add item properties
    if item_info:
        markdown.append("## Properties\n")
        for key, value in item_info.items():
            if key != 'name':
                markdown.append(f"- **{key}:** {value}")
        markdown.append("")
    
    # Add combat stats
    if combat_stats:
        markdown.append("## Combat Styles\n")
        markdown.append("| Style | Attack Type | Combat Style | Speed |")
        markdown.append("|-------|-------------|--------------|-------|")
        for style in combat_stats['combat_styles']:
            markdown.append(f"| {style['name']} | {style['attack_type']} | {style['style']} | {style['speed']} |")
        markdown.append("")
    
    # Add bonuses
    if bonuses:
        markdown.append("## Equipment Bonuses\n")
        
        markdown.append("### Attack Bonuses")
        for type, value in bonuses['attack'].items():
            markdown.append(f"- **{type.title()}:** {value}")
        markdown.append("")
        
        markdown.append("### Defense Bonuses")
        for type, value in bonuses['defense'].items():
            markdown.append(f"- **{type.title()}:** {value}")
        markdown.append("")
        
        markdown.append("### Other Bonuses")
        for type, value in bonuses['other'].items():
            markdown.append(f"- **{type.title()}:** {value}")
            
    return "\n".join(markdown)

# Example usage with the provided context

with open("wiki_pages/Abyssal_whip.html", 'r', encoding='utf-8') as file:
    html_content = file.read()

markdown_output = html_to_markdown(html_content)
print(markdown_output)