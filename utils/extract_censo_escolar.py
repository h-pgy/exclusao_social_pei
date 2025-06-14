from typing import Optional
import requests
import os
from tqdm import tqdm
from config import RANGE_ANOS_ESTUDO, DATA_FOLDER

class ExtractCensoEscolar:

    URL_DOWNLOAD = 'https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{ano}.zip'

    def __init__(self, range_anos:Optional[list[int]]=None):
         
         self.range_anos = range_anos or RANGE_ANOS_ESTUDO

    def solve_file_microdados_censo(self, ano:int) -> str:

            fname = f'microdados_censo_{ano}.zip'
            if not os.path.exists(DATA_FOLDER):
                os.mkdir(DATA_FOLDER)
            fpath = os.path.join(DATA_FOLDER, fname)

            return fpath


    def download_microdados_censo(self, ano:int)->str:

        link_download_microdados_censo = self.URL_DOWNLOAD.format(ano=ano)
        fpath = self.solve_file_microdados_censo(ano)
        with requests.get(link_download_microdados_censo, stream=True) as r:
            r.raise_for_status()
            with open(fpath, 'wb') as f:
                content = r.content
                f.write(content)
                
        return fpath
    
    def pipeline(self)->list[str]:

        files = []
        print('Iniciando o download dos microdados do censo escolar...')
        ja_baixados = []
        baixados = []
        for ano in tqdm(self.range_anos):
            file = self.solve_file_microdados_censo(ano)
            if not os.path.exists(file):
                baixados.append(ano)
                self.download_microdados_censo(ano)
            else:
                ja_baixados.append(ano)

        print(f'Microdados do censo escolar já baixados previamente para os anos: {ja_baixados}')
        print(f'Microdados do censo escolar baixados agora para os anos: {baixados}')
        return files

    def __call__(self)->list[str]:
        """
        Executa o pipeline de download dos microdados do censo escolar.
        """
        return self.pipeline()
