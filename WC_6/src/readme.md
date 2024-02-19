# Crawler

A brief description of the project.

## Table of Contents

- [Description](#Description)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Description



## Usage

#### Steps to run the project

- Step 1
Create a list with Companies and Websites to crawl
this WebSites must have a www.example.com format. (protocol is not needed)
the protocol will be added automatically.

- Step 2
Set the settings (on the Setting section) to the desired values, following the instructions as follows:
```python

SOURCE_FILE_NAME='Url_Cata_raw.xlsx'
SORT_WORDS_LIST = ['CONTACT','ABOUT']
NUMBER_OF_THREADS = 10
CRAWLED_SIZE_LIMIT = 50
LINKS_LIMIT = 25
CHUNK_SIZE = 500

```
SOURCE_FILE_NAME: 
the name of the file with the list of companies and websites to crawl.

SORT_WORDS_LIST:
the list of words to search on the crawled websites, this list will be used to sort the crawled websites.

NUMBER_OF_THREADS:
the number of threads to use on the crawling process.

CRAWLED_SIZE_LIMIT:
the number of websites to crawl. This Limit will be applied to each company. to limit the total number of crawled websites.

LINKS_LIMIT:
the number of links to crawl on each website.

CHUNK_SIZE:
the number of websites to crawl on each thread.

## Contributing

Guidelines for contributing to the project and how to submit pull requests.

## License

Information about the project's license and any usage restrictions.

## Contact

Contact information for the project maintainer or team.
