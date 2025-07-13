# lightweight-etl-pipeline-to-gcp


Python-based ETL pipeline orchestrator designed for environments where Airflow is unavailable (due to permissions, infrastructure constraints, or complexity). It provides a no-frills, dependency-light way to define, schedule, and monitor ETL workflows using Python libraries.

Objectives
- Works in Restricted Environments – No need for Airflow, Docker, or complex setups.
- Easy to Deploy – Runs anywhere Python runs (even on locked-down servers).
- Minimal Overhead – Perfect for small scale ETL needs.

Features
- Task Dependencies – Simple depends_on syntax for execution order.
- Basic Retry Logic – Automatic retries for failed tasks.
- Logging & Failure Alerts – Log to file and send email/Slack alerts (optional).
- Flexible Scheduling – Works with schedule, APScheduler, or cron.

Project Structure


Setup
- Installation....

Steps
bigquery/bigquery_loader
(arruamr schema)
4-orchestration/pipeline_runner
4-orchestration/scheduler
5-dashboard/streamlit_app

inserir no orquestrador:
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
python -m src.orchestration.pipeline_runner


source .venv/Scripts/activate
C:/Users/kajin/Downloads/workspace/lightweight-etl-pipeline-to-gcp/.venv/Scripts/python.exe -m src.orchestration.pipeline_runner



-------------
 Passo a Passo: Criar e usar uma Service Account no Google Cloud
1. Acesse o Console do Google Cloud
Vá para:
👉 https://console.cloud.google.com/

2. Selecione ou crie um projeto
No topo da tela, clique no seletor de projeto e:

Crie um novo projeto (ex: meu-projeto-dados)
ou

Escolha um já existente.

3. Habilite a API do Google Cloud Storage
Acesse:
👉 https://console.cloud.google.com/apis/library/storage.googleapis.com

Clique em "Ativar" se ainda não estiver ativada.

4. Crie uma Service Account
Vá para:
👉 https://console.cloud.google.com/iam-admin/serviceaccounts

Clique em "+ Criar Conta de Serviço"

Preencha os campos:

Nome: servico-dados

ID da conta de serviço: (deixe o que ele gerar)

Clique em Criar e continuar

Conceda as permissões:

Papel: Storage Admin (isso permite ler e escrever no bucket)

Clique em Continuar e depois Concluir

5. Crie uma chave JSON para a Service Account
Na lista de contas de serviço, clique sobre a conta que acabou de criar.

Vá até a aba Chaves.

Clique em "Adicionar chave" → "Criar nova chave"

Escolha o tipo JSON

Clique em Criar – o download do arquivo .json começará automaticamente.

📁 Guarde esse arquivo com segurança! Ele é sua credencial.