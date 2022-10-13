import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

# explicação começa na Aula05 aos 15min

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "R_Bafica",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 13)
}


@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'])
def trab02_dag1():

    @task
    def ingestao():
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False,
                  sep=";")
        return NOME_DO_ARQUIVO

    @task
    def ind_passageiros(nome_do_arquivo):
        NOME_TABELA = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg({
            "PassengerId": "count"}).reset_index()
        print(res)

        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def ind_tarifas(nome_do_arquivo):
        NOME_TABELA2 = "/tmp/preco_medio_tarifa_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg({
            "Fare": "mean"}).reset_index()
        print(res)

        res.to_csv(NOME_TABELA2, index=False, sep=";")
        return NOME_TABELA2

    @task
    def ind_parentes(nome_do_arquivo):
        NOME_TABELA3 = "/tmp/soma_parentes_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        df['SibSp_Parch'] = df['SibSp'] + df['Parch']
        res = df.groupby(['Sex', 'Pclass']).agg({
            "SibSp_Parch": "sum"}).reset_index()
        print(res)

        res.to_csv(NOME_TABELA3, index=False, sep=";")
        return NOME_TABELA3

    @task
    def ind_tab_unica(NOME_TABELA, NOME_TABELA2, NOME_TABELA3):
        TABELA_UNICA = "/tmp/tabela_unica.csv"

        df = pd.read_csv(NOME_TABELA, sep=";")
        df2 = pd.read_csv(NOME_TABELA2, sep=";")
        df3 = pd.read_csv(NOME_TABELA3, sep=";")

        df12 = df.merge(df2, on=['Sex', 'Pclass'], how='inner')
        df123 = df12.merge(df3, on=['Sex', 'Pclass'], how='inner')
        print(df123)
        df123.to_csv(TABELA_UNICA, index=False, sep=";")
        return TABELA_UNICA

    inicio = DummyOperator(task_id="inicio")
    fim = DummyOperator(task_id="fim")

    triggerdag = TriggerDagRunOperator(
        task_id="trigga_trabalho_02_ex02",
        trigger_dag_id="trab02_dag2")

    ing = ingestao()
    passageiros = ind_passageiros(ing)
    tarifas = ind_tarifas(ing)
    parentes = ind_parentes(ing)
    tab_unica = ind_tab_unica(passageiros, tarifas, parentes)

    inicio >> ing >> [passageiros, tarifas,
                      parentes] >> tab_unica >> fim >> triggerdag


execucao = trab02_dag1()
