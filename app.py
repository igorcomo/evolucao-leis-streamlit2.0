# app.py
import time
import math
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Evolução das Proposições – Câmara dos Deputados", page_icon="📜", layout="wide")

# -------- sessão HTTP com retry/backoff + headers ----------
def make_session():
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.2,  # 1.2s, 2.4s, 4.8s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "FGV-ProgWeb-P2-IgorCosta/1.0",
        "Accept": "application/json",
    })
    return s

SESSION = make_session()
BASE = "https://dadosabertos.camara.leg.br/api/v2"

def safe_get(url, params=None, timeout=30):
    """GET com tratamento de 429 e retorno seguro (dict ou None)."""
    try:
        resp = SESSION.get(url, params=params, timeout=timeout)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", "2"))
            time.sleep(wait)
            resp = SESSION.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        st.warning(f"Servidor retornou {resp.status_code} para {url}. Vou tentar prosseguir com dados parciais.")
        return None
    except Exception as e:
        st.warning(f"Falha ao acessar {url}: {e}")
        return None

# --------- CAMADA DE DADOS ---------

@st.cache_data(ttl=3600, show_spinner=False)
def buscar_pls_periodo(ano_ini: int, ano_fim: int) -> pd.DataFrame:
    """
    Coleta TODOS os PLs do período (paginação completa).
    Câmara: /proposicoes?siglaTipo=PL&dataApresentacaoInicio=YYYY-01-01&dataApresentacaoFim=YYYY-12-31&itens=100&pagina=N
    """
    todos = []
    for ano in range(ano_ini, ano_fim + 1):
        pagina = 1
        while True:
            params = {
                "siglaTipo": "PL",
                "dataApresentacaoInicio": f"{ano}-01-01",
                "dataApresentacaoFim": f"{ano}-12-31",
                "itens": 100,
                "pagina": pagina,
                # dica: poderia filtrar outras flags aqui se desejar
            }
            js = safe_get(f"{BASE}/proposicoes", params)
            if not js:
                break

            dados = js.get("dados", [])
            todos.extend(dados)

            # segue pelo link "next" se existir
            links = {l["rel"]: l["href"] for l in js.get("links", [])}
            if "next" in links and len(dados) > 0:
                pagina += 1
                time.sleep(0.15)  # não estourar rate-limit
            else:
                break

    df = pd.DataFrame(todos)
    # normaliza datas auxiliares para gráficos
    if "dataApresentacao" in df.columns:
        df["dataApresentacao"] = pd.to_datetime(df["dataApresentacao"], errors="coerce", utc=True)
        df["ano_mes"] = df["dataApresentacao"].dt.to_period("M").astype(str)
        df["ano"] = df["dataApresentacao"].dt.year
    return df

@st.cache_data(ttl=3600, show_spinner=False)
def autores_por_proposicao(id_proposicao: int):
    """Retorna lista de autores para 1 proposição (cada item = dict)."""
    js = safe_get(f"{BASE}/proposicoes/{id_proposicao}/autores")
    if not js:
        return []
    return js.get("dados", [])

@st.cache_data(ttl=3600, show_spinner=False)
def partido_do_deputado(id_deputado: int) -> str:
    """
    Busca o partido do deputado NO ENDPOINT DE DEPUTADOS.
    O professor avisou: o partido NÃO vem em /autores. Aqui usamos /deputados/{id}
    e pegamos ultimoStatus.siglaPartido.
    """
    js = safe_get(f"{BASE}/deputados/{id_deputado}")
    if not js:
        return ""
    dados = js.get("dados", {}) or {}
    ultimo = dados.get("ultimoStatus", {}) or {}
    return (ultimo.get("siglaPartido") or "").upper()

@st.cache_data(ttl=3600, show_spinner=False)
def contagem_por_partido(df_pl: pd.DataFrame, usar_todos: bool) -> pd.DataFrame:
    """
    Conta autores parlamentares por partido.
    Para evitar milhares de chamadas, quando usar_todos=False limitamos a amostra.
    Quando usar_todos=True, percorre TODAS as proposições do período.
    """
    ids = df_pl.get("id", pd.Series(dtype="int64")).dropna().astype(int).tolist()
    if not usar_todos:
        # amostra rápida (aumente/diminua se quiser)
        ids = ids[:800]

    partidos = []
    for i, pid in enumerate(ids, start=1):
        if i % 25 == 0:
            time.sleep(0.4)  # espaçar requisicoes

        autores = autores_por_proposicao(pid)
        for a in autores:
            # considera somente parlamentares
            if (a.get("tipoAutor") or "").lower().startswith("parlamentar"):
                # o id do deputado pode vir como "id" dentro de "autor" ou "idDeputado"
                idep = a.get("idDeputado") or (a.get("autor") or {}).get("id")
                if idep:
                    p = partido_do_deputado(int(idep))
                    if p:
                        partidos.append(p)

    s = pd.Series(partidos, dtype="string")
    if s.empty:
        return pd.DataFrame(columns=["partido", "autores"])
    out = s.value_counts().rename_axis("partido").reset_index(name="autores")
    out = out.sort_values("autores", ascending=False).reset_index(drop=True)
    return out

# --------- UI ---------

st.title("📜 Evolução das Proposições – Câmara dos Deputados (API Oficial)")

colA, colB = st.columns(2)
with colA:
    ano_ini = st.number_input("Ano inicial", min_value=1991, max_value=2025, value=2019, step=1)
with colB:
    ano_fim = st.number_input("Ano final", min_value=ano_ini, max_value=2025, value=2025, step=1)

st.caption("Fonte: https://dadosabertos.camara.leg.br/ (proposições e autores)")

usar_todos = st.toggle("Usar TODOS os PLs para o gráfico por partido (lento, mais completo)", value=False)
if st.button("Atualizar dados", type="primary"):
    st.session_state["do_fetch"] = True

do_fetch = st.session_state.get("do_fetch", True)

if do_fetch:
    with st.spinner("Buscando PLs (com paginação completa)…"):
        df = buscar_pls_periodo(ano_ini, ano_fim)

    if df.empty:
        st.error("Não veio nenhum PL para o período escolhido. Tente outro intervalo.")
        st.stop()

    # --- Gráfico 1: evolução mensal por ano-mês ---
    st.subheader("Evolução de PLs apresentados (por mês)")
    evol = df["ano_mes"].value_counts().sort_index().rename_axis("ano_mes").reset_index(name="PLs")
    fig1 = px.line(evol, x="ano_mes", y="PLs", markers=True)
    st.plotly_chart(fig1, use_container_width=True)
    st.caption("Obs.: mostramos a **data de apresentação** (ano-mês), como o professor solicitou.")

    # --- Gráfico 2: contagem por partido (autores parlamentares) ---
    st.subheader("PLs por partido dos autores (deputados)")
    with st.spinner("Contando autores por partido… (pode demorar se 'Usar TODOS' estiver ligado)"):
        cont = contagem_por_partido(df, usar_todos)

    if cont.empty:
        st.warning("Não foi possível determinar partidos (autores não encontrados ou limite da API). Tente ligar 'Usar TODOS' e/ou reduzir o intervalo de anos.")
    else:
        fig2 = px.bar(cont, x="partido", y="autores")
        st.plotly_chart(fig2, use_container_width=True)
        st.caption("Regra: buscamos os **autores parlamentares** de cada PL e, para cada **deputado**, coletamos o **partido** no endpoint `/deputados/{id}` (campo `ultimoStatus.siglaPartido`).")

    # Tabela (amostra) e links úteis
    with st.expander("Ver amostra de PLs coletados"):
        st.dataframe(df[["id", "siglaTipo", "numero", "ano", "ementa"]].head(50), use_container_width=True)

    st.markdown("**Links oficiais da API (Câmara):**")
    st.markdown("- Documentação: https://dadosabertos.camara.leg.br/swagger/api.html")
    st.markdown("- Exemplo de proposição: https://dadosabertos.camara.leg.br/api/v2/proposicoes/257161")
