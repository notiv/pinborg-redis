
import io

from bs4 import BeautifulSoup
from pypdf import PdfReader

def parse_pdf(response):
    """ Parses a pdf url (response) and returns the content as text. Images and 
    other elements are ignored.

    Parameters
    ----------
    response : A Scrapy Response object with a .pdf suffix (indicating a pdf 
    url) 

    Returns
    -------
        A string with the content of the pdf (only text) 
    """

    if response.url[-4:] != '.pdf':
        raise ValueError(f'''{response.url} is not a pdf (it must have .pdf 
         as suffix.''') 

    mem_file = io.BytesIO(response.body)
    reader = PdfReader(mem_file)
    content = ''
    for page in reader.pages:
        text = page.extract_text()
        content = ' '.join([content, text])

    return content

def parse_html(response):
    """Parses the html response and returns the content as text. Images and 
    other elements are ignored.

    Parameters
    ----------
    response : A Scrapy Response object 

    Returns
    -------
        A string with the content of the html page (only text)
    """

    soup = BeautifulSoup(response.body, 'html.parser')
    # http://stackoverflow.com/questions/22799990/beatifulsoup4-get-text-still-has-javascript
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)

    return text
