import pandas as pd
from conexao_banco import conexao_aws
import os
from datetime import datetime


class dre_indicadores:

    def __init__(self, primeiro_dia_indicador, ultimo_dia_indicador, tuple_cod_empresas = ()):
        
        usuario_sql = os.getenv('usuario_sql')
        senha_sql = os.getenv('senha_sql')
        self.aws = conexao_aws(senha = senha_sql, usuario=usuario_sql, nome_do_banco='edu_db')
        self.aws.iniciar_conexao()
        self.primeiro_dia_indicador = datetime.strptime(primeiro_dia_indicador, '%Y-%m-%d').date()
        self.ultimo_dia_indicador = datetime.strptime(ultimo_dia_indicador , '%Y-%m-%d').date()
        self.setores_restritos = ['Holding', 'Ignorados']

        if tuple_cod_empresas != ():
           
            self.todos_os_ativos = False
            self.tuple_cod_empresas = tuple_cod_empresas

        else:
            
            self.todos_os_ativos = True


    def coletando_balancos_cvm(self):

        if self.todos_os_ativos:

            self.demonstracoes_sql = pd.read_sql(f'''SELECT c.id_doc, c.data_reb, c.data_ref, d.con_ind, d.numero_conta,
                                    d.nome_conta, d.valor_conta, c.cod_empresa, d.tipo_dem, c.tipo_doc
                                    FROM cadastro_dem AS c
                                    LEFT JOIN dados_financeiros AS d USING(id_doc)
                                    WHERE  d.tipo_dem in (6, 7) AND
                                    (c.data_ref >= '{self.primeiro_dia_indicador}' AND c.data_ref < '{self.ultimo_dia_indicador}') '''
                                    , con=self.aws.engine)


        else:

            try:

                self.demonstracoes_sql = pd.read_sql(f'''SELECT c.id_doc, c.data_reb, c.data_ref, d.con_ind, d.numero_conta,
                                        d.nome_conta, d.valor_conta, c.cod_empresa, d.tipo_dem, c.tipo_doc
                                        FROM cadastro_dem AS c
                                        LEFT JOIN dados_financeiros AS d USING(id_doc)
                                        WHERE  d.tipo_dem in (6, 7) AND
                                        (c.data_ref >= '{self.primeiro_dia_indicador}' AND c.data_ref < '{self.ultimo_dia_indicador}') AND c.cod_empresa = {self.tuple_cod_empresas}'''
                                        , con=self.aws.engine)

            except:

                self.demonstracoes_sql = pd.read_sql(f'''SELECT c.id_doc, c.data_reb, c.data_ref, d.con_ind, d.numero_conta,
                                        d.nome_conta, d.valor_conta, c.cod_empresa, d.tipo_dem, c.tipo_doc
                                        FROM cadastro_dem AS c
                                        LEFT JOIN dados_financeiros AS d USING(id_doc)
                                        WHERE  d.tipo_dem in (6, 7) AND
                                        (c.data_ref >= '{self.primeiro_dia_indicador}' AND c.data_ref < '{self.ultimo_dia_indicador}') AND c.cod_empresa in {self.tuple_cod_empresas}'''
                                        , con=self.aws.engine)

        self.documentos = (self.demonstracoes_sql.sort_values(by = 'data_ref', ascending=True))['id_doc'].unique()

    def coletando_dados_de_cadastro(self):

        self.cadastro_empresas = pd.read_sql('''SELECT cod, cod_empresa, setor from cadastro_empresas''', con= self.aws.engine)

    def coletando_dados_fundamentalistas(self):

        self.dados_fundamentalistas_antigos = pd.read_sql('''SELECT * FROM dados_fundamentalistas''', con = self.aws.engine)

        self.dados_fundamentalistas_antigos["data_reb"] = pd.to_datetime(self.dados_fundamentalistas_antigos["data_reb"])
        self.dados_fundamentalistas_antigos["data_ref"] = pd.to_datetime(self.dados_fundamentalistas_antigos["data_ref"]).dt.date

    def pegando_indicadores_dfc(self):  

        self.aws.iniciar_conexao()

        self.id_dado_fundamentalista = pd.read_sql('''SELECT id_dado_fundamentalista FROM dados_fundamentalistas''', con=self.aws.engine)['id_dado_fundamentalista'].to_list()

        self.documentos = list(filter(lambda x: x > 12950, self.documentos))
                                                                 
        for documento in self.documentos:

            self.lista_df_indicadores = []

            print(documento)

            self.coletando_dados_fundamentalistas()

            demonstracao_completa = self.demonstracoes_sql.query(f"id_doc ==  {documento}")

            nome_documento = demonstracao_completa.iat[0, 9].upper()

            dfp = nome_documento == "DFP"

            if 2 in demonstracao_completa['con_ind'].to_list():

                if demonstracao_completa[(demonstracao_completa['con_ind'] == 2) & (demonstracao_completa['numero_conta'].str.contains("6.01"))]['valor_conta'].iat[0] != 0:

                    demonstracao_completa = demonstracao_completa.query("con_ind == 2")

                elif len(set(demonstracao_completa['valor_conta'])) == 1:

                    try:

                        demonstracao_completa = demonstracao_completa.query("con_ind == 2")
                    
                    except:

                        demonstracao_completa = demonstracao_completa.query("con_ind == 1")

                else:

                    demonstracao_completa = demonstracao_completa.query("con_ind == 1")

            setor_empresa = self.cadastro_empresas.query(f"cod_empresa == {demonstracao_completa.iat[0, 7]}").setor.unique()

            empresa_viavel = (str(setor_empresa[0]) not in self.setores_restritos)

            id_doc = demonstracao_completa.iat[0, 0]
            data_reb = demonstracao_completa.iat[0, 1]
            data_ref = demonstracao_completa.iat[0, 2]
            cod_empresa = str(demonstracao_completa.iat[0, 7])

            if empresa_viavel:

                lista_indicadores = ['depreciação_e_amortização', 'dividendos_e_jcp', 'capex', 'fco', 'fcf', 'fcl_capex'] 
                                    
                lista_id_3m = [f"{cod_empresa}_{id_doc}_{indicador}_3m" for indicador in lista_indicadores]
                lista_id_12m = [f"{cod_empresa}_{id_doc}_{indicador}_12m" for indicador in lista_indicadores]

                if dfp:

                    lista_nomes_capex = self.nomes_capex(df = demonstracao_completa)
                    lista_nomes_deprecicação = self.nomes_depreciação(df = demonstracao_completa)
                    lista_nomes_dividendos = self.nomes_dividendos(df = demonstracao_completa)

                    lista_atributos = self.coletando_indicadores(df = demonstracao_completa, 
                                                                nome_capex = lista_nomes_capex,
                                                                nome_depreciacao = lista_nomes_deprecicação,                                                                
                                                                nome_dividendos = lista_nomes_dividendos)


                    for i, indicador in enumerate(lista_indicadores):

                        self.lista_df_indicadores.append(pd.DataFrame({'id_dado_fundamentalista': lista_id_12m[i],
                                            'data_reb': data_reb,
                                            'data_ref': data_ref,
                                            'cod_empresa': cod_empresa,
                                            'id_doc': id_doc,
                                            'nome_indicador': indicador,
                                            'periodicidade': '12m',
                                            'valor': lista_atributos[i],
                                            'tipo_doc': "DFP"}, index= [0]))
                        
                    possui_refencias = self.verificando_a_existencia_de_3_referencias_anteriores(cod_empresa, data_reb, data_ref)

                    if possui_refencias:

                        lista_atributos = self.puxando_dados_9m(self.referencias_para_filtro, self.referencias_antigas, nome_documento, lista_atributos)

                        for i, indicador in enumerate(lista_indicadores):

                            self.lista_df_indicadores.append(pd.DataFrame({'id_dado_fundamentalista': lista_id_3m[i],
                                'data_reb': data_reb,
                                'data_ref': data_ref,
                                'cod_empresa': cod_empresa,
                                'id_doc': id_doc,
                                'nome_indicador': indicador,
                                'periodicidade': '3m',
                                'valor': lista_atributos[i],
                                'tipo_doc': "DFP"}, index= [0]))
                                

                else:

                    lista_trimestres_passados = self.verificando_qual_trimestre_atual(cod_empresa, data_reb, data_ref)

                    if lista_trimestres_passados == []:

                        lista_nomes_capex = self.nomes_capex(df = demonstracao_completa)
                        lista_nomes_deprecicação = self.nomes_depreciação(df = demonstracao_completa)
                        lista_nomes_dividendos = self.nomes_dividendos(df = demonstracao_completa)

                        lista_atributos_3m = self.coletando_indicadores(df = demonstracao_completa, 
                                                                    nome_capex = lista_nomes_capex,
                                                                    nome_depreciacao = lista_nomes_deprecicação,                                                                
                                                                    nome_dividendos = lista_nomes_dividendos)
                    
                    else:

                        lista_nomes_capex = self.nomes_capex(df = demonstracao_completa)
                        lista_nomes_deprecicação = self.nomes_depreciação(df = demonstracao_completa)
                        lista_nomes_dividendos = self.nomes_dividendos(df = demonstracao_completa)

                        lista_atributos = self.coletando_indicadores(df = demonstracao_completa, 
                                                                    nome_capex = lista_nomes_capex,
                                                                    nome_depreciacao = lista_nomes_deprecicação,                                                                
                                                                    nome_dividendos = lista_nomes_dividendos)

                        lista_atributos_3m = self.puxando_dados_9m(lista_trimestres_passados, self.referencias_antigas, nome_documento, lista_atributos)


                    for i, indicador in enumerate(lista_indicadores):

                            self.lista_df_indicadores.append(pd.DataFrame({'id_dado_fundamentalista': lista_id_3m[i],
                                'data_reb': data_reb,
                                'data_ref': data_ref,
                                'cod_empresa': cod_empresa,
                                'id_doc': id_doc,
                                'nome_indicador': indicador,
                                'periodicidade': '3m',
                                'valor': lista_atributos_3m[i],
                                'tipo_doc': "ITR"}, index= [0]))

                    possui_refencias = self.verificando_a_existencia_de_3_referencias_anteriores(cod_empresa, data_reb, data_ref)

                    if possui_refencias:

                        lista_atributos_12m = self.puxando_dados_9m(self.referencias_para_filtro, self.referencias_antigas, nome_documento, lista_atributos_3m, pontual=False)

                        for i, indicador in enumerate(lista_indicadores):

                            self.lista_df_indicadores.append(pd.DataFrame({'id_dado_fundamentalista': lista_id_12m[i],
                                'data_reb': data_reb,
                                'data_ref': data_ref,
                                'cod_empresa': cod_empresa,
                                'id_doc': id_doc,
                                'nome_indicador': indicador,
                                'periodicidade': '12m',
                                'valor': lista_atributos_12m[i],
                                'tipo_doc': "ITR"}, index= [0]))
            
            self.colocando_indicadores_na_base()


                    


    def verificando_qual_trimestre_atual(self, cod_empresa, data_reb, data_ref):

        referencias_antigas = self.dados_fundamentalistas_antigos[(self.dados_fundamentalistas_antigos["cod_empresa"] == cod_empresa)]
        referencias_antigas = referencias_antigas[referencias_antigas["data_reb"] < data_reb]
        referencias_antigas = referencias_antigas[referencias_antigas["data_ref"] < data_ref]

        self.referencias_antigas = referencias_antigas

        referencias_para_filtro = referencias_antigas.sort_values(by = "data_ref", ascending=False).drop_duplicates("data_ref")

        referencias_passadas = []

        if referencias_para_filtro.empty:

            return referencias_passadas

        for i, data in enumerate(referencias_para_filtro['data_ref'].to_list()):

            if referencias_para_filtro.iloc[i, 8].upper() == "DFP":

                return referencias_passadas

            elif i > 2:

                print('algo está errado')

            else:

                referencias_passadas.append(data)


    def verificando_a_existencia_de_3_referencias_anteriores(self, cod_empresa, data_reb, data_ref):

        referencias_antigas = self.dados_fundamentalistas_antigos[(self.dados_fundamentalistas_antigos["cod_empresa"] == cod_empresa)]
        referencias_antigas = referencias_antigas[referencias_antigas["data_reb"] < data_reb]
        referencias_antigas = referencias_antigas[referencias_antigas["data_ref"] < data_ref]
        referencias_antigas = referencias_antigas[referencias_antigas["periodicidade"] == "3m"]

        referencias_para_filtro = referencias_antigas['data_ref'].sort_values(ascending=False).unique()

        possui_refencias = True

        referencias_para_filtro = referencias_para_filtro[:3]

        if len(referencias_para_filtro) != 3:

            possui_refencias = False

        self.referencias_antigas = referencias_antigas
        self.referencias_para_filtro = referencias_para_filtro

        return possui_refencias

    def puxando_dados_9m(self, referencias_para_filtro, referencias_antigas, nome_documento, lista_atributo, pontual = True):

        depre_9m = 0
        dividendo_9m = 0
        capex_9m = 0
        fco_9m = 0
        fcf_9m = 0
        fcl_capex_9m = 0

        referencias_antigas = referencias_antigas[referencias_antigas["periodicidade"] == "3m"]

        for referencia in referencias_para_filtro:

            dado_mais_att = referencias_antigas[referencias_antigas['data_ref'] == referencia]

            dado_mais_att = dado_mais_att[dado_mais_att['data_reb'] == max(dado_mais_att['data_reb'])]

            depre_9m = dado_mais_att.query("nome_indicador == 'depreciação_e_amortização' ")['valor'].iat[0] + depre_9m

            dividendo_9m = dado_mais_att.query("nome_indicador == 'dividendos_e_jcp' ")['valor'].iat[0] + dividendo_9m

            capex_9m = dado_mais_att.query("nome_indicador == 'capex' ")['valor'].iat[0] + capex_9m

            fco_9m = dado_mais_att.query("nome_indicador == 'fco' ")['valor'].iat[0] + fco_9m

            fcf_9m = dado_mais_att.query("nome_indicador == 'fcf' ")['valor'].iat[0] + fcf_9m

            fcl_capex_9m = dado_mais_att.query("nome_indicador == 'fcl_capex' ")['valor'].iat[0] + fcl_capex_9m

        lista_atributos_calculado = []

        lista_atributos_9m = [depre_9m, dividendo_9m, capex_9m, fco_9m,
                                fcf_9m, fcl_capex_9m]

        for a, atributo in enumerate(lista_atributo):

            if nome_documento == "DFP" or pontual == True:

                dado = atributo - lista_atributos_9m[a]

                lista_atributos_calculado.append(dado)

            else:

                dado = atributo + lista_atributos_9m[a]

                lista_atributos_calculado.append(dado)

        return lista_atributos_calculado
    
        
    def nomes_capex(self, df):

        df = df[df['numero_conta'].str.contains("6.02")]

        lista_nome_capex_filtro_1 = ['imobilizado', 'imobilizados', 'intangível', 'intangíveis', 'intangivel', 'intangiveis', 'investimento', 'investimentos']

        lista_restrita_filtro_1 = ['mais valia', 'venda','juros','hedge','ágio','cotas','capital','depósito', 'exterior', 'circulante','coligada', 'coligadas', 'alienação', 'alienações', 'venda', 'vendas', 'recebimento', 'baixa', 'custo', 'provisão', 'amortização', 
                                    'amortizações', 'perda', 'aienado', 'alienados', 'controladas', 'realização', 'fundos', 'fundo', 'resultado', 
                                    'capitalizado', "ganho", "ganhos", "recebido", "recebidos", 'recebimento', 'recebimentos', "caixa", "titulos",
                                    "atividade", "atividades", "títulos", 'título', 'redução', 'reduções', 'outros', 'curto', 'prazo', 'relacionada',
                                    'relacionadas', 'partes', 'participação', 'participações', 'ações', 'incorporação', 'incorporações', 'aplicações', 'aplicação']
                        
        lista_nome_capex_final_filtro_1 = [s for s in df['nome_conta'] if any(word.title() in s for word in lista_nome_capex_filtro_1)] 

        lista_nome_capex_final_filtro_1_miniscula = [s for s in df['nome_conta'] if any(word in s for word in lista_nome_capex_filtro_1)]

        lista_nome_capex_final_filtro_1.extend(lista_nome_capex_final_filtro_1_miniscula) 

        nome_capex_inuteis_filtro_1 = [s for s in lista_nome_capex_final_filtro_1 if any(word.title() in s for word in lista_restrita_filtro_1)] 

        nome_capex_inuteis_2 = [s for s in lista_nome_capex_final_filtro_1 if any(word in s for word in lista_restrita_filtro_1)] 

        nome_capex_inuteis_filtro_1.extend(nome_capex_inuteis_2)

        lista_nome_capex_final_filtro_1 = list(filter(lambda x: x not in nome_capex_inuteis_filtro_1, lista_nome_capex_final_filtro_1))

        lista_nome_capex_filtro_2 = ['adições', 'adição', 'aquisições', 'aquisição', 'compras', 'compra', 'aplicação', 'aplicações', 'custo', 'custos', 'ações no', 'ação no']

        lista_nome_capex_filtro_2_complementar = ['imobilizado', 'intangível', 'ativos biológicos', 'investimento', 'imobilizados', 'intangíveis', 'permanente', 'empresas']

        lists_restrita = ['mais valia', 'vendas', 'venda','cotas','ágio','controladas', 'realização', 'fundos', 'fundo', 'resultado', 'capitalizado', "ganho", "ganhos", "recebido", "recebidos",
                        'recebimento', 'recebimentos', 'obrigação', 'obrigações']

        lista_nome_capex_final_filtro_2_1m = [s for s in df['nome_conta'] if any(word.title() in s for word in lista_nome_capex_filtro_2) and any(word in s for word in lista_nome_capex_filtro_2_complementar)]
        
        lista_nome_capex_final_filtro_2_1m_2 = [s for s in df['nome_conta'] if any(word in s for word in lista_nome_capex_filtro_2) and any(word.title() in s for word in lista_nome_capex_filtro_2_complementar)]

        lista_nome_capex_final_filtro_2_maiuscula = [s for s in df['nome_conta'] if any(word.title() in s for word in lista_nome_capex_filtro_2) and any(word.title() in s for word in lista_nome_capex_filtro_2_complementar)]     

        lista_nome_capex_final_filtro_2_minuscula = [s for s in df['nome_conta'] if any(word in s for word in lista_nome_capex_filtro_2) and any(word in s for word in lista_nome_capex_filtro_2_complementar)] 

        lista_nome_capex_final_filtro_2 = lista_nome_capex_final_filtro_2_1m

        lista_nome_capex_final_filtro_2.extend(lista_nome_capex_final_filtro_2_1m_2)
  
        lista_nome_capex_final_filtro_2.extend(lista_nome_capex_final_filtro_2_maiuscula)

        lista_nome_capex_final_filtro_2.extend(lista_nome_capex_final_filtro_2_minuscula)
  
        nome_capex_inuteis = [s for s in lista_nome_capex_final_filtro_2 if any(word.title() in s for word in lists_restrita)] 

        nome_capex_inuteis_2 = [s for s in lista_nome_capex_final_filtro_2 if any(word in s for word in lists_restrita)] 

        nome_capex_inuteis.extend(nome_capex_inuteis_2)

        lista_nome_capex_final_filtro_2 = list(filter(lambda x: x not in nome_capex_inuteis, lista_nome_capex_final_filtro_2))

        lista_nome_capex_final_filtro_1.extend(lista_nome_capex_final_filtro_2)

        lista_nome_capex_final = set(lista_nome_capex_final_filtro_1)

        return lista_nome_capex_final

    def nomes_dividendos(self, df):

        df = df[df['numero_conta'].str.contains("6.03")]

        lista_nome_capex_filtro_1 = ['dividendo', 'dividendos', 'próprio', 'JCP', 'dividentos', 'pagamento acionista', 'pgto acionista', 'remuneração acionista', 'remunerações acionista',
                                    'pagamento aos acionistas', 'pgto acionista', 'remuneração aos acionistas', 'remunerações ao acionista', 'remuneração dos acionistas', 'proprio', 'provento'
                                    ,'proventos']

        lista_restrita_filtro_1 = ['empréstimo', 'empréstimos', 'debêntures', 'captação', 'recursos', 'financiamento', 'financiamentos', 'variação']

        lista_nome_dividendos_filtro_1 = [s for s in df['nome_conta'] if any(word.title() in s for word in lista_nome_capex_filtro_1)] 

        lista_nome_dividendos_filtro_1_miniscula = [s for s in df['nome_conta'] if any(word in s for word in lista_nome_capex_filtro_1)]

        lista_nome_dividendos_filtro_1.extend(lista_nome_dividendos_filtro_1_miniscula) 

        nome_capex_inuteis_filtro_1 = [s for s in lista_nome_dividendos_filtro_1 if any(word.title() in s for word in lista_restrita_filtro_1)]

        nome_capex_inuteis_2 = [s for s in lista_nome_dividendos_filtro_1 if any(word in s for word in lista_restrita_filtro_1)] 

        nome_capex_inuteis_filtro_1.extend(nome_capex_inuteis_2)

        lista_nome_dividendos_filtro_1 = list(filter(lambda x: x not in nome_capex_inuteis_filtro_1, lista_nome_dividendos_filtro_1))

        lista_nome_dividendos = set(lista_nome_dividendos_filtro_1)

        return lista_nome_dividendos


    def nomes_depreciação(self, df):

        df = df[df['numero_conta'].str.contains("6.01")]

        lista_nome_depreciacao_filtro_1 = ['depreciação', 'depreciações', 'amortização', 'amortizações', 'exaustão']

        lista_restrita_filtro_1 = ['direito', 'custos', 'dívidas', 'dividas', 'despesas', 'custo', 'outros', 'debêntures',
                                    'créditos', 'cŕedito', 'arrendamento', 'mercantil', 'despesa', 'gasto', 'gastos', 'dívida']
                        
        lista_nome_depreciacao_final_filtro_1 = [s for s in df['nome_conta'] if any(word.title() in s for word in lista_nome_depreciacao_filtro_1)] 

        lista_nome_depreciacao_final_filtro_1_miniscula = [s for s in df['nome_conta'] if any(word in s for word in lista_nome_depreciacao_filtro_1)]

        lista_nome_depreciacao_final_filtro_1.extend(lista_nome_depreciacao_final_filtro_1_miniscula) 

        nome_capex_inuteis_filtro_1 = [s for s in lista_nome_depreciacao_final_filtro_1 if any(word.title() in s for word in lista_restrita_filtro_1)] 

        nome_capex_inuteis_2 = [s for s in lista_nome_depreciacao_final_filtro_1 if any(word in s for word in lista_restrita_filtro_1)] 

        nome_capex_inuteis_filtro_1.extend(nome_capex_inuteis_2)

        lista_nome_depreciacao_final_filtro_1 = list(filter(lambda x: x not in nome_capex_inuteis_filtro_1, lista_nome_depreciacao_final_filtro_1))

        lista_nome_depreciacao = set(lista_nome_depreciacao_final_filtro_1)

        return lista_nome_depreciacao

    def coletando_indicadores(self, df, nome_capex, nome_dividendos, nome_depreciacao):

        capex = 0
        dividendo = 0
        depreciacao = 0
        fco = 0
        fcf = 0

        if nome_capex != ():

            df_capex = df[df['numero_conta'].str.contains("6.02")]

            for nome in nome_capex:

                capex = capex + df_capex[df_capex['nome_conta'] == nome]['valor_conta'].iloc[0]

        if nome_dividendos != ():
            
            df_dividendo = df[df['numero_conta'].str.contains("6.03")]

            for nome in nome_dividendos:

                dividendo = dividendo + df_dividendo[df_dividendo['nome_conta'] == nome]['valor_conta'].iloc[0]

        if nome_depreciacao != ():

            df_depre = df[df['numero_conta'].str.contains("6.01")]

            for nome in nome_depreciacao:

                depreciacao = depreciacao + df_depre[df_depre['nome_conta'] == nome]['valor_conta'].iloc[0]


        try:

            fco = df[df['numero_conta'] == '6.01']['valor_conta'].iloc[0]

        except:

            pass

        try:

            fcf = df[df['numero_conta'] == '6.03']['valor_conta'].iloc[0]

        except:
            
            pass


        fcl_capex = fco - capex

        return [depreciacao, dividendo, capex, fco, fcf, fcl_capex]

    
    def colocando_indicadores_na_base(self):

        if self.lista_df_indicadores != []:

            dados_fundamentalistas = pd.concat(self.lista_df_indicadores)

            dados_fundamentalistas['data_reb'] = pd.to_datetime(dados_fundamentalistas['data_reb'])
            dados_fundamentalistas['data_ref'] = pd.to_datetime(dados_fundamentalistas['data_ref'])

            dados_fundamentalistas = dados_fundamentalistas[~dados_fundamentalistas['id_dado_fundamentalista'].isin(self.id_dado_fundamentalista)]
            dados_fundamentalistas = dados_fundamentalistas.reset_index(drop=True)
            dados_fundamentalistas = dados_fundamentalistas.drop_duplicates(["id_dado_fundamentalista"])

            dados_fundamentalistas.to_sql('dados_fundamentalistas', self.aws.engine, index=False, if_exists='append', chunksize=10000, method='multi')


if __name__ == "__main__":

    lista_anos_iniciais = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
    lista_anos_finais = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]

    for i, item in enumerate(lista_anos_finais):
        
        print(f'passou por {i}')

        data_inicio_do_periodo = f'20{lista_anos_iniciais[i]}-12-30' #digitar nesse formato
        data_final_do_periodo = f'20{lista_anos_finais[i]}-12-30'

        teste_indicador = dre_indicadores(primeiro_dia_indicador= data_inicio_do_periodo, ultimo_dia_indicador= data_final_do_periodo)

        teste_indicador.coletando_balancos_cvm()
        teste_indicador.coletando_dados_de_cadastro()
        teste_indicador.pegando_indicadores_dfc()

    # df_final = pd.concat(teste_indicador.lista_df_indicadores)

    # print(df_final)

    #teste_indicador.colocando_indicadores_na_base()

    #648, 832, 451, 323, 810, 275, 174, 148, 734, 591, 37