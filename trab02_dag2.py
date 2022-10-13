import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from datetime import datetime

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "R_Bafica",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 9)
}


@dag(default_args=default_args, schedule_interval=None, catchup=False, tags=['DAG2'])
def trab02_dag2():

    @task
    def tab_unica():
        NOME_DO_ARQUIVO = "/tmp/tabela_unica.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        df.to_csv(NOME_DO_ARQUIVO, index=False, header=True, sep=";")
        return NOME_DO_ARQUIVO

    @task
    def media_indicadores(NOME_DO_ARQUIVO):
        TABELA_INDICADORES = "/tmp/resultados.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        res = df.groupby(['Sex']).agg(
            {"PassengerId": "mean", "Fare": "mean", "SibSp_Parch": "mean"}).reset_index()
        print("\nMédia dos valores de passageiros, tarifas e numero de parentes, por sexo:\n", res)

        res1 = df.agg(
            {"PassengerId": "mean", "Fare": "mean", "SibSp_Parch": "mean"}).reset_index()
        print("\nMédia dos valores de passageiros, tarifas e numero de parentes:\n", res1)

        res.to_csv(TABELA_INDICADORES, index=False, sep=";")
        return TABELA_INDICADORES

    inicio = DummyOperator(task_id="inicio")
    fim = DummyOperator(task_id="fim")

    df_tab_unica = tab_unica()
    df_media_indicadores = media_indicadores(df_tab_unica)

    inicio >> df_tab_unica >> df_media_indicadores >> fim


execucao = trab02_dag2()
