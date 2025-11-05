from usp.tree import sitemap_tree_for_homepage
import pandas as pd

tree = sitemap_tree_for_homepage('https://www.fc-mado.com/')

urls = []
for page in tree.all_pages():
    urls.append(page.url)

df = pd.DataFrame(urls)
df.to_csv("all_urls.csv", index=False)
