import requests
from bs4 import BeautifulSoup
import pandas as pd

class PEITableParser:

    URL_POST_ESCOLAS_PEI = 'https://transparencia.educacao.sp.gov.br/Home/conteudoTabelaPrograma'

    PAYLOAD_BUSCA = dict(
    tipo=1,
    #diretoria=,
    #escola=,
    #municipio=,
    #endereco=,
    programa=27 #id do programa PEI
    )

    def get_table_from_pei_website(self) ->str:
        """
        Fetches the PEI data from the website and returns its html.
        """
        with requests.post(self.URL_POST_ESCOLAS_PEI, data=self.PAYLOAD_BUSCA) as response:
            if response.status_code == 200:
                table = response.text
                return table
            else:
                error_msg = f'Falha ao buscar a tabela. Error code: {response.status_code}. Error msg: {response.text}'
                raise RuntimeError(error_msg)

    def get_table_element(self, html_table):
        """
        Parses the HTML table and returns the table element.
        """
        soup = BeautifulSoup(html_table, 'html.parser')
        table = soup.find('table')
        
        if not table:
            raise ValueError("No table found in the HTML content.")

        return table

    def get_cols(self, table) -> list[str]:
        return [th.text.strip() for th in table.find_all('tr')[0].find_all('th')]
    
    
    #extrai as coordenadas do link do google maps
    def parse_coords_link_google(self, link_google)->list[str]:
        return link_google.split('/')[-3].split(',')
    
    # extrai o id da escola do link para mais detalhes
    def parse_id_escola(self, link_detalhes: str) -> str:
        return link_detalhes.split('?')[-1].replace('codesc=', '')
    
    def parse_normal_data(self, td) -> str:
        """
        Parses a table cell that contains normal text data.
        """
        return td.text.strip()
    
    def get_links_from_last_column(self, cells)->list[str]:

        last_col_data = cells[-1]

        link_data = last_col_data.find_all('a') if last_col_data else []
        actual_links = [link['href'] for link in link_data if 'href' in link.attrs]

        return actual_links

    def parse_table(self, table) -> pd.DataFrame:
        """
        Parses the HTML table and returns a DataFrame.
        """
        cols = self.get_cols(table)
        cols.remove('Ações')  # Remove the last column header as it contains links
        cols.append('Coordenadas')
        cols.append('ID Escola')
        data = []

        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            row_data = [self.parse_normal_data(td) for td in cells[:-1]]
            links = self.get_links_from_last_column(cells)
            coords = self.parse_coords_link_google(links[0])
            id_escola = self.parse_id_escola(links[1])
            row_data.append(coords)
            row_data.append(id_escola)

            data.append(row_data)

        return pd.DataFrame(data, columns=cols)
    
    def pipeline(self) -> pd.DataFrame:
        """
        Main pipeline to fetch, parse and return the PEI data as a DataFrame.
        """
        html_table = self.get_table_from_pei_website()
        table = self.get_table_element(html_table)
        df = self.parse_table(table)
        return df
    
    def __call__(self) -> pd.DataFrame:
        """
        Allows the class instance to be called as a function to get the DataFrame.
        """
        return self.pipeline()
