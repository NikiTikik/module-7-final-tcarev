Airflow UI: http://localhost:8081 (admin/admin)

ClearML UI: http://localhost:8080 (admin@clearml.com/password)

API: http://localhost:8008

Files: http://localhost:8081

# Запуск
docker compose up -d

# Проверка работы
docker compose ps

В DAG используйте Task.init() из clearml
Эксперименты появляются в ClearML UI автоматически
ClearML Agent в Kubernetes

# Как это работает
```text
1. Airflow DAG → Task.init(queue="default") 
         ↓
2. ClearML Server (localhost:8080) регистрирует задачу
         ↓  
3. K8s Agent (namespace mlops-agent) подхватывает из очереди "default"
         ↓
4. Agent запускает задачу в Pod (Docker/K8s executor)
         ↓
5. Результаты (модели, метрики) → ClearML UI
```

Чарт агента ClearML находится тут
https://github.com/clearml/clearml-helm-charts/tree/main/charts/clearml-agent 

# 1. Добавить официальный репозиторий ClearML
```
helm repo add clearml https://clearml.github.io/clearml-helm-charts
helm repo update
```

# 2. Создать Secret с credentials (из ClearML UI)
```
kubectl create secret generic clearml-agent-creds \
  --from-literal=access_key=YOUR_ACCESS_KEY \
  --from-literal=secret_key=YOUR_SECRET_KEY \
  -n clearml
```

# 3. Создать ConfigMap
```
kubectl create configmap clearml-agent-config \
  --from-literal=api_host=clearml-apiserver:8008 \
  --from-literal=web_host=clearml-webserver:8080 \
  --from-literal=files_host=clearml-fileserver:8081 \
  -n clearml
  ```

# 4. Установить
```
helm install clearml-agent clearml/clearml-agent \
  --namespace clearml \
  --create-namespace \
  -f values.yaml
```
```text
Airflow (ETL) → ClearML Pipeline → K8s Agent (ML) → ClearML UI (результаты)
        ↓
   E2E MLOps в одном клике
```
