from classes.scraping_bs4 import WebScrapingBS4
from core.database import Database
from classes.bot_logger import BotHealthManager
from datetime import datetime, timedelta
from time import sleep
from classes.config import Config

bot_manager = BotHealthManager()
logging = bot_manager.logger
config = Config()

data_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Calcular a data de corte (3 dias atrás)
cutoff_date = datetime.now() - timedelta(days=3)

def main():

    scraping1 = WebScrapingBS4(link=config.FONTE_NOTICIAS_CIENTIFICAS)
    noticias = scraping1.perga_texto(elementoFILHO='a', elementoPAI='div', tipoPAI='class', descricaoPAI='feed-post-body')
    links = scraping1.pega_url(elementoFILHO='a', elementoPAI='div', tipoPAI='class', descricaoPAI='feed-post-body')

    numero_de_coletas: int = 1
    with Database() as db:
        collection = db.get_collection('newsLetter', 'noticiasCientificas')
        for noticia, link in zip(noticias, links):

            bot_manager.add_registro_analisado()
            logging.info(f'Coleta {numero_de_coletas} inicida.')
            numero_de_coletas += 1
            
            sleep(0.1)
            verifica_cadastro_na_base = collection.find_one({"noticia": noticia})
            if verifica_cadastro_na_base:
                logging.info("Registro já cadastrado anteriormente...")
                collection.update_one({"noticia": noticia}, {"$set": {"data_atualizacao": data_atual}})
                continue

            # como é uma newsletter, não há necessicade de manter coletas antigas
            collection.delete_many({'date_cadastro': {'$lt': cutoff_date}})
            
            collection.insert_one({'noticia': noticia,
                                   "link": link,
                                   "date_cadastro": data_atual,
                                   "data_atualizacao": data_atual})
            
            logging.info('Coleta persistida com sucesso!')
            bot_manager.add_registro_persistido()

    bot_manager.finalizar_execucao(st_sucesso_execucao=True)


if __name__ == "__main__":
    main()
