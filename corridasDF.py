from classes.scraping_bs4 import WebScrapingBS4
from bs4 import BeautifulSoup
from urllib.request import urlopen
from classes.bot_logger import BotHealthManager
from datetime import datetime
from classes.funcoes_apoio import converter_data
from core.database import Database
from classes.config import Config


bot_manager = BotHealthManager()
logging = bot_manager.logger
config = Config()


def main():

    data_atualizacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    coleta = config.FONTE_CORRIDAS_DF
    site = WebScrapingBS4(f'{coleta}')
    link = site.pega_url(elementoPAI='div', tipoPAI='class',
                         descricaoPAI='evo_event_schema', elementoFILHO='a',
                         tipoFILHO='itemprop', descricaoFILHO='url')
    logging.info(f"Coleta iniciada em: {coleta}")

    numero_de_coletas = 1
    with Database() as db:
        collection = db.get_collection('newsLetter', 'corridasDF')
        for url in link:

            bot_manager.add_registro_analisado()

            logging.info(f'Coleta {numero_de_coletas} inicida.')
            numero_de_coletas += 1
            html = urlopen(f'{url}')
            soup = BeautifulSoup(html.read(), 'html.parser')

            nome_do_evento = soup.find(name='span', attrs=({'class': 'evoet_title evcal_desc2 evcal_event_title'})).text
            distancia = soup.find(name='div', attrs=({'class': 'evo_custom_content evo_data_val'})).text
            local = soup.find(name='p', attrs=({'class': 'evo_location_name'})).text
            horario = soup.find(name='span', attrs=({'class': 'evo_eventcard_time_t'})).text
            data_evento = soup.find(name='span', attrs=({'class': 'evo_start'})).text
            data_evento = data_evento[0:2] + data_evento[2:5]
            data_evento = converter_data(data_evento)

            """
                A estrutura desta página conta com duas divs idênticas, por isso foi necessário esse tratamento para pegar o valor da inscrição da corrida.
            """
            parse_valor = soup.find_all(name='div', attrs=({'class': 'evo_custom_content evo_data_val'}))
            if len(parse_valor) == 2:
                valor = parse_valor[1].get_text()
            else:
                valor = None

            link = soup.find(name='a', attrs=({'class': 'evcal_evdata_row evo_clik_row'}))
            inscricao = link.get('href')
            
    
            verifica_cadastro_na_base = collection.find_one({"nome": nome_do_evento})
            if verifica_cadastro_na_base:
                logging.info('Já cadastrado. Verificando valor e data do evento...')
                # como já está cadastrado, nesse momento ele vai atualizar o date.
                collection.update_one({"nome": nome_do_evento}, {"$set": {"data_atualizacao": data_atualizacao}})

                verifica_se_valor_da_corrida_cadastrado = collection.find_one({
                    "nome": nome_do_evento,
                    "valor": valor,
                    "data_evento": data_evento
                })
                if not verifica_se_valor_da_corrida_cadastrado:
                    collection.update_one({"nome": nome_do_evento}, {"$set": {"valor":valor, "data_evento": data_evento}})
                    logging.info('Atualizado com sucesso.')
                else:
                    logging.info('Coleta já cadastrada anteriormente.')
                continue
            
            else:
                collection.insert_one({
                        "nome": nome_do_evento,
                        "distancia": distancia,
                        "local": local,
                        "valor": valor,
                        "horario": horario,
                        "inscricao": inscricao,
                        "data_atualizacao": data_atualizacao,
                        "data_cadastro": data_atualizacao,
                        "data_evento": data_evento
                    })

            logging.info('Coleta persistida com sucesso!')
            bot_manager.add_registro_persistido()

        bot_manager.finalizar_execucao(st_sucesso_execucao=True)


if __name__ == "__main__":
    main()