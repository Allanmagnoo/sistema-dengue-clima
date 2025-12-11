# Política de Segurança de Dados e Credenciais

Este documento define as diretrizes e procedimentos para o gerenciamento de dados sensíveis no projeto **Sistema Dengue-Clima**.

---

## 1. Classificação de Dados Sensíveis

São considerados dados sensíveis e requerem proteção especial:

*   **Credenciais de Acesso:**
    *   Senhas de Banco de Dados (PostgreSQL).
    *   Tokens de API (InfoDengue, INMET, etc.).
    *   Chaves de acesso Cloud (AWS, Google Cloud).
    *   Chaves SSH (`.pem`, `id_rsa`).
*   **Configurações de Infraestrutura:**
    *   Arquivos `.env` contendo segredos.
    *   Arquivos `config.yaml` ou `settings.json` com senhas em texto plano.
*   **Dados Pessoais (PII):**
    *   Embora o projeto foque em dados agregados, qualquer arquivo contendo nomes, CPFs ou endereços exatos deve ser tratado como restrito.

---

## 2. Armazenamento Seguro (`.secrets/`)

Foi criada uma estrutura de diretórios protegida para armazenar esses ativos:

*   **Diretório:** `.secrets/` (na raiz do projeto).
*   **Permissões:** Este diretório está incluído no `.gitignore` e não deve ser versionado.
*   **Estrutura:** O diretório espelha, quando necessário, a estrutura de arquivos original ou armazena os arquivos de configuração centralizados.

### Como acessar credenciais no código

Ao invés de hardcode (chumbar) senhas no código, utilize variáveis de ambiente carregadas do arquivo `.env` localizado em `.secrets/`.

**Exemplo (Python):**

```python
from dotenv import load_dotenv
import os
from pathlib import Path

# Caminho seguro
project_root = Path(__file__).resolve().parents[2] # Ajuste conforme profundidade
secure_env = project_root / ".secrets" / ".env"

load_dotenv(secure_env)

db_password = os.getenv("DB_PASSWORD")
```

---

## 3. Scanner e Automação de Segurança

O projeto conta com um utilitário de verificação e proteção automática: `src/utils/security_scanner.py`.

### Funcionalidades
1.  **Varredura:** Analisa recursivamente o projeto buscando arquivos sensíveis (`.env`, `*.pem`, `*_credentials.json`).
2.  **Proteção:** Move automaticamente os arquivos detectados para a pasta `.secrets/`.
3.  **Auditoria:** Gera logs da operação e mantém um manifesto (`.secrets/manifest.json`) mapeando a origem e destino dos arquivos.

### Execução Manual
Para rodar a verificação de segurança manualmente:

```bash
python src/utils/security_scanner.py
```

---

## 4. Prevenção (Pre-commit)

Para evitar que dados sensíveis sejam enviados acidentalmente para o repositório remoto (GitHub), recomenda-se o uso de hooks do Git.

### Configuração do Hook (Windows/Linux)

Crie o arquivo `.git/hooks/pre-commit` (sem extensão) com o seguinte conteúdo:

```bash
#!/bin/sh
echo "Executando verificação de segurança..."
python src/utils/security_scanner.py

# Verifica se o scanner encontrou/moveu arquivos novos (código de saída ou log)
# Por simplicidade, o scanner move arquivos. Se você tentar commitar um arquivo que acabou de ser movido, o git avisará que ele sumiu.
```

*Nota: Em ambientes Windows, a execução direta de scripts shell no hook pode exigir configuração do Git Bash.*

---

## 5. Plano de Resposta a Incidentes

Caso um dado sensível seja acidentalmente commitado e enviado ao repositório remoto:

1.  **Revogação Imediata:**
    *   Cancele a chave/token/senha exposta no provedor de serviço imediatamente.
    *   Gere uma nova credencial.

2.  **Limpeza do Histórico:**
    *   Não basta deletar o arquivo em um novo commit; ele permanece no histórico.
    *   Utilize ferramentas como `BFG Repo-Cleaner` ou `git filter-branch` para remover o arquivo de todo o histórico do Git.
    *   **Atenção:** Isso reescreve o histórico do projeto e pode afetar outros desenvolvedores.

3.  **Atualização:**
    *   Coloque a nova credencial na pasta `.secrets/`.
    *   Avise a equipe sobre a rotação das chaves.

---
**Responsável pela Segurança:** Equipe de Engenharia de Dados
**Última Atualização:** 11/12/2025
