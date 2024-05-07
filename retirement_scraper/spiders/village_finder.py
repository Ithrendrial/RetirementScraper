import scrapy
from scrapy.linkextractors import LinkExtractor
import requests
import pandas as pd
from bs4 import BeautifulSoup

start_url = "https://www.retirementvillages.org.nz/Site/RVA_Villages/Default.aspx"
location_url_pattern = r'https://www.retirementvillages.org.nz/tools/clients/directory\.aspx\?SECT=[^#]+'

site_data = []  # List to store all scraped content from site, from all locations

class VillageFinderSpider(scrapy.Spider):
    name = "village_finder"

    def start_requests(self):
        yield scrapy.Request(url=start_url, callback=self.parse)

    def parse(self, response):
        # Retrieve the urls for each location in NZ
        url_extractor = LinkExtractor(allow=location_url_pattern)
        location_urls = url_extractor.extract_links(response)

        # Iterate through each location and scrape its data
        for location_url in location_urls:
            yield scrapy.Request(url=location_url.url, callback=self.parse_location)

    def parse_location(self, response):
        if response.status_code != 200:
            raise Exception(f"Failed to retrieve the webpage. Status code: {response.status_code}")

        # Parse the HTML document to retrieve info for each village
        soup = BeautifulSoup(response.content, 'html.parser')

        villages = soup.find_all('table')
        for village in villages:
            rows = village.find_all('tr')
            village_data = {}  # Data scraped from single retirement village
            for row in rows:
                columns = row.find_all('td')
                for column in columns:
                    if column.get('class') is None:
                        continue
                    class_name = column.get('class')
                    if not isinstance(class_name, str):
                        class_name = " ".join(class_name)
                    txt = column.get_text()
                    village_data[class_name] = txt
            site_data.append(village_data)  # Adds data from each village to the final data list

        df = pd.DataFrame(site_data)
        df.rename(columns={
            'villagesCol VillageAddress': 'Address',
            'villagesCol VillageOrganisation': 'Name',
            'villagesCol VillagePhone': 'Phone+Fax',
            'villagesCol VillageAge': 'Minimum Age',
            'villagesCol': 'Electorate',
        }, inplace=True)
        df = df.drop(columns=df.columns.difference({'Address', 'Name', 'Phone+Fax', 'Minimum Age', 'Electorate'}))

        # Clean address
        df['Address'] = df['Address'].str.replace('Street Address: ', '')
        df['Address'] = df['Address'].str.strip()

        # Clean phone+fax to separate phone and fax columns
        df['Phone+Fax'] = df['Phone+Fax'].str.replace(' ', '')
        df['Phone+Fax'] = df['Phone+Fax'].str.replace('-', '')
        df['Phone'] = df['Phone+Fax'].str.extract(r'Phone:(\d+)')
        df['Fax'] = df['Phone+Fax'].str.extract(r'Fax:(\d+)')
        df['Phone'] = df['Phone'].fillna(df['Phone+Fax'])
        df = df.drop(columns=['Phone+Fax'])

        # remove "Electorate: " from Electorate column
        df['Electorate'] = df['Electorate'].str.replace('Electorate: ', '')

        # remove "Minimum Age Entry for New Residents: " from Minimum Age column
        df['Minimum Age'] = df['Minimum Age'].str.replace('Minimum Age Entry for New Residents: ', '')

        df.to_csv('retirement_data', index=False)
        print(f"Data saved to retirement_data")


