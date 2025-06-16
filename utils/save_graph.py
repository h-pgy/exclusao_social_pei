import matplotlib.pyplot as plt
from config import GRAPHS_FOLDER
import os

def save_graph(fig, file_name: str, dpi: int = 300, bbox_inches: str = 'tight') -> None:
    """
    Salva o gráfico atual em um arquivo PNG.

    Parâmetros:
    - file_name: caminho e nome do arquivo (ex: 'grafico.png')
    - dpi: resolução da imagem (padrão: 300)
    - bbox_inches: define o recorte do conteúdo (padrão: 'tight' para evitar cortes)
    """

    if not os.path.exists(GRAPHS_FOLDER):
        os.mkdir(GRAPHS_FOLDER)

    file_name = os.path.join(GRAPHS_FOLDER, file_name)
    if not file_name.endswith('.png'):
        file_name += '.png'

    fig.savefig(file_name, dpi=dpi, bbox_inches=bbox_inches)