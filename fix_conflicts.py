import re

with open('backend/scraper-agent-254.js', 'r', encoding='utf-8') as f:
    content = f.read()

# Pattern to match conflict blocks and keep the HEAD version
pattern = r'<<<<<<< HEAD\n(.*?)\n=======\n.*?\n>>>>>>> [^\n]+\n'
cleaned = re.sub(pattern, r'\1\n', content, flags=re.DOTALL)

with open('backend/scraper-agent-254.js', 'w', encoding='utf-8') as f:
    f.write(cleaned)

print("✅ Conflict markers removed!")
