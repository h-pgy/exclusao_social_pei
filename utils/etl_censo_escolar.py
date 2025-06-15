
import pandas as pd
import os
from zipfile import ZipFile
from tqdm import tqdm
from typing import Optional


from utils.save_csv import save_csv
from .extract_censo_escolar import ExtractCensoEscolar

from config import RANGE_ANOS_ESTUDO, DATA_FOLDER

class ETLMicrodadosCensoEscolar:


    def __init__(self, range_anos:Optional[list[int]]=None) -> None:

        self.range_anos = range_anos or RANGE_ANOS_ESTUDO
        self.extract_censo = ExtractCensoEscolar(self.range_anos)

    @property
    def cols_mapper(self) -> dict[str, str]:
        """
        Mapeia as colunas de interesse para os nomes originais.
        """
        indices = {
            'NU_ANO_CENSO' : 'ano_censo', 
            'SG_UF' : 'sigla_uf', 
            'NO_MUNICIPIO' : 'nome_municipio',
            'CO_MUNICIPIO' : 'codigo_municipio', 
            'CO_ENTIDADE' : 'codigo_escola', 
            'NO_ENTIDADE' : 'nome_escola', 
        }
        totais = {
            'QT_TUR_BAS' : 'qtd_turmas_ed_basica', #quantidade de turmas da educação básica
            'QT_MAT_BAS' : 'qtd_matr_ed_basica', #quantidade de matrículas da educação básica
            'QT_MAT_MED' : 'qtd_matr_medio', #quantidade de matrículas do ensino médio
        }
        matriculas_basica_cor = {
            'QT_MAT_BAS_ND' : 'qtd_matr_ed_basica_nao_declarada',
            'QT_MAT_BAS_BRANCA' : 'qtd_matr_ed_basica_branca',
            'QT_MAT_BAS_PRETA' : 'qtd_matr_ed_basica_preta',
            'QT_MAT_BAS_PARDA' : 'qtd_matr_ed_basica_parda',
            'QT_MAT_BAS_AMARELA' : 'qtd_matr_ed_basica_amarela',
            'QT_MAT_BAS_INDIGENA' : 'qtd_matr_ed_basica_indigena',
        }
        matriculas_integrais = {
            'QT_MAT_INF_INT' : 'qtd_matriculas_infantil_integral',
            'QT_MAT_INF_CRE_INT' : 'qtd_matriculas_infantil_creche_integral',
            'QT_MAT_INF_PRE_INT' : 'qtd_matriculas_infantil_pre_escolar_integral',
            'QT_MAT_FUND_INT' : 'qtd_matriculas_fundamental_integral',
            'QT_MAT_FUND_AI_INT' : 'qtd_matriculas_fundamental_anos_iniciais_integral',
            'QT_MAT_FUND_AF_INT' : 'qtd_matriculas_fundamental_anos_finais_integral', #esse é do PEI
            'QT_MAT_MED_INT' : 'qtd_matriculas_medio_integral', # esse também
        }

        return {**indices, **totais, **matriculas_basica_cor, **matriculas_integrais}

    @property
    def cols_interesse(self) -> list[str]:
        return list(self.cols_mapper.keys())

    def solve_data_folder_within_zip(self, ano:int) -> str:

        folder_map = {
            2022: f'Microdados do Censo Escolar da Educaç╞o Básica {ano}',
            2023: f'microdados_censo_escolar_{ano}',
            2024 : f'microdados_censo_escolar_{ano}'
        }

        folder = folder_map.get(ano, f'microdados_ed_basica_{ano}')
        return folder
    
    def solve_csv_data_path_within_zip(self, ano:int) -> str:
    
        fname: str = f'microdados_ed_basica_{ano}.csv'
        
        folder = self.solve_data_folder_within_zip(ano)
        
        path: str = os.path.join(folder, 'dados', fname)

        return path

    def read_data(self, ano:int)->pd.DataFrame:

        fpath = self.extract_censo.solve_file_microdados_censo(ano)
        if not os.path.exists(fpath):
            raise FileNotFoundError(f'Arquivo {fpath} não encontrado. Baixe os dados primeiro.')

        zip_file_path = self.solve_csv_data_path_within_zip(ano)
        with ZipFile(fpath, 'r') as zip_ref:
            if zip_file_path not in zip_ref.namelist():
                #por alguma razão, o arquivo pode estar com extensão .CSV em vez de .csv
                zip_file_path = zip_file_path.replace('.csv', '.CSV')
                #caso nao encontre mesmo assim, levanta o erro
                if zip_file_path not in zip_ref.namelist():
                    raise FileNotFoundError(f'Arquivo {zip_file_path} não encontrado no zip {fpath}.')
            with zip_ref.open(zip_file_path) as file:
                df = pd.read_csv(file, sep=';', encoding='latin1', low_memory=False)
            return df
        
    def filtrar_sp(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra o DataFrame para incluir apenas os dados do estado de São Paulo.
        """
        df = df[df['CO_UF'] == 35].copy()  # 35 é o código do estado de São Paulo
        return df
    
    def filtrar_escolas_estaduais(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra o DataFrame para incluir apenas as escolas estaduais.
        """
        # 2 é o código para escolas estaduais
        df = df[df['TP_DEPENDENCIA'] == 2].copy()
        return df
    
    def filtrar_escolas_em_atividade(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra o DataFrame para incluir apenas as escolas que estão ativas.
        """
        # 1 é o código para escolas ativas
        df = df[df['TP_SITUACAO_FUNCIONAMENTO'] == 1].copy()
        return df
    
    def filtrar_escolas_fund_II(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra o DataFrame para incluir apenas as escolas que oferecem ensino fundamental II.
        """
        df_fund_II = df[df['IN_FUND_AF'] == 1].copy()  # 1 é o código para ensino fundamental II
        return df_fund_II
    
    def filtrar_escolas_escolas_medio(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra o DataFrame para incluir apenas as escolas que oferecem ensino médio.
        """
        df_medio = df[df['IN_MED'] == 1].copy()  # 1 é o código para ensino médio
        return df_medio
    
    def remover_escolas_tecnico(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove as escolas que oferecem ensino técnico.
        """
        df_sem_tecnico = df[df['IN_PROF_TEC'] == 0].copy()  # 0 é o código para não oferecer ensino técnico
        return df_sem_tecnico
    
    def filtrar_cols_interesse(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra o DataFrame para incluir apenas as colunas de interesse.
        """
        df = df[self.cols_interesse].copy()
        return df
    
    def assert_cols_match(self, df: pd.DataFrame) -> None:
        """
        Verifica se as colunas do DataFrame correspondem às colunas de interesse.
        """
        if not (df.columns == self.cols_interesse).all():
            raise ValueError(f'As colunas do DataFrame não correspondem às colunas de interesse.'
                            f'Esperadas: {self.cols_interesse}, encontradas: {df.columns.tolist()}')
        
    def map_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Mapeia as colunas do DataFrame para os nomes originais.
        """
        
        self.assert_cols_match(df)
        df = df.rename(columns=self.cols_mapper)
        return df

    def pipeline(self) -> pd.DataFrame:
        """
        Executa o pipeline de extração, transformação e carregamento dos microdados do censo escolar.
        """

        print('Iniciando o pipeline de ETL dos microdados do censo escolar...')
        self.extract_censo.pipeline()  # Garante que os dados foram baixados
        df_final = pd.DataFrame()
        print('Lendo e processando os dados...')
        for ano in tqdm(self.range_anos):
            df = self.read_data(ano)
            #filtros
            df = self.filtrar_sp(df)
            df = self.filtrar_escolas_estaduais(df)
            df = self.filtrar_escolas_em_atividade(df)
            # nao queremos mais escoal fund II, só médio
            #df = self.filtrar_escolas_fund_II(df)
            df = self.filtrar_escolas_escolas_medio(df)
            df = self.remover_escolas_tecnico(df)
            #limpeza
            df = self.filtrar_cols_interesse(df)
            df_final = pd.concat([df_final, df], ignore_index=True)

        df_final = self.map_cols(df_final)
        print('Pipeline de ETL concluído.')

        return df_final
    

    @property
    def save_file_path(self) -> str:
        """
        Retorna o caminho do arquivo CSV onde os dados serão salvos.
        """
        return 'microdados_censo_limpo.csv'
    
    def __call__(self, save_csv_file=True) -> pd.DataFrame:
        """
        Executa o pipeline de extração, transformação e carregamento dos microdados do censo escolar.
        """

        fpath = self.save_file_path
        if os.path.exists(os.path.join(DATA_FOLDER, fpath)):
            print(f'Arquivo com dados limpos já existe. Carregando os dados do arquivo...')
            df = pd.read_csv(os.path.join(DATA_FOLDER, fpath))
            return df

        df = self.pipeline()
        if save_csv_file:
            save_csv(df, 'microdados_censo_limpo.csv')
        return df
            