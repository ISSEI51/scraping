import pandas as pd
from usp.tree import sitemap_tree_for_homepage

tree = sitemap_tree_for_homepage("https://dairitenboshu.com/")

urls = []
for page in tree.all_pages():
    urls.append(page.url)

df = pd.DataFrame(urls)
df.to_csv("all_urls.csv", index=False)
