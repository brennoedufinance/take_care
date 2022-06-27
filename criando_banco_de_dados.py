from conexao_banco import conexao_aws
import os



usuario_sql = os.getenv('usuario_sql')
senha_sql = os.getenv('senha_sql')
aws = conexao_aws(senha = senha_sql, usuario=usuario_sql, nome_do_banco='edu_db')
aws.iniciar_conexao()



query = '''CREATE TABLE dados_fundamentalistas (id_dado_fundamentalista VARCHAR(50),
                                                data_reb date,
                                                data_ref date,
                                                cod_empresa VARCHAR(5),
                                                id_doc INT,
                                                nome_indicador VARCHAR(50),
                                                periodicidade VARCHAR(20),
                                                valor INT,
                                                tipo_doc VARCHAR(5),
                                                PRIMARY key(id_dado_fundamentalista)) '''

aws.cursor.execute(query)
