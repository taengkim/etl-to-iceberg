# Helm Charts for Airflow ETL Stack

이 폴더에는 ArgoCD를 통해 Kubernetes 클러스터에 배포할 수 있는 Helm 차트와 ArgoCD Application 매니페스트가 포함되어 있습니다.

## 폴더 구조

```
helm/
├── argoapps/          # ArgoCD Application 매니페스트
│   ├── postgresql-app.yaml
│   ├── redis-app.yaml
│   ├── airflow-app.yaml
│   └── kustomization.yaml
├── postgresql/        # PostgreSQL values 파일
│   └── values.yaml
├── redis/            # Redis values 파일
│   └── values.yaml
└── airflow/          # Airflow values 파일
    └── values.yaml
```

## 구성 요소

### 1. PostgreSQL (Cloud Native PostgreSQL)
- **Chart**: Bitnami PostgreSQL
- **용도**: Airflow 메타데이터 데이터베이스
- **버전**: 15.1.2
- **네임스페이스**: airflow

### 2. Redis (Sentinel Redis)
- **Chart**: Bitnami Redis
- **용도**: Celery 메시지 브로커
- **버전**: 19.1.0
- **네임스페이스**: airflow
- **아키텍처**: Standalone (Sentry Redis 대신)

### 3. Apache Airflow
- **Chart**: Apache Airflow
- **버전**: 1.13.1
- **Executor**: CeleryExecutor
- **네임스페이스**: airflow
- **컴포넌트**:
  - Webserver (1 replica)
  - Scheduler (1 replica)
  - Celery Workers (2 replicas)
  - Flower (1 replica) - Celery 모니터링

## 설치 방법

### ArgoCD를 통한 설치

1. **ArgoCD Repository 추가** (Helm chart 저장소):
```bash
# Helm chart 저장소 추가
argocd repo add https://charts.bitnami.com/bitnami --type helm --name bitnami
argocd repo add https://airflow.apache.org --type helm --name airflow
```

2. **ArgoCD Application 배포**:
```bash
# PostgreSQL 배포
kubectl apply -f helm/argoapps/postgresql-app.yaml

# Redis 배포
kubectl apply -f helm/argoapps/redis-app.yaml

# Airflow 배포 (PostgreSQL, Redis 이후)
kubectl apply -f helm/argoapps/airflow-app.yaml
```

또는 모든 Application을 한 번에 배포:
```bash
kubectl apply -k helm/argoapps/
```

### 수동 설치 (Helm CLI)

1. **Repository 추가**:
```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

2. **PostgreSQL 설치**:
```bash
helm install postgresql bitnami/postgresql \
  --namespace airflow \
  --create-namespace \
  -f helm/postgresql/values.yaml
```

3. **Redis 설치**:
```bash
helm install redis bitnami/redis \
  --namespace airflow \
  -f helm/redis/values.yaml
```

4. **Airflow 설치**:
```bash
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  -f helm/airflow/values.yaml
```

## 설정 커스터마이징

### 공통 설정 변경

#### PostgreSQL 비밀번호 변경
```yaml
# helm/postgresql/values.yaml
auth:
  postgresPassword: <새로운 비밀번호>
  password: <새로운 비밀번호>
```

#### Redis 비밀번호 추가
```yaml
# helm/redis/values.yaml
auth:
  enabled: true
  password: <비밀번호>
```

#### Airflow 리소스 조정
```yaml
# helm/airflow/values.yaml
workers:
  replicas: 3  # Worker 수 증가
  
scheduler:
  resources:
    limits:
      memory: "2Gi"  # 메모리 증가
```

### Git Sync 설정

DAG 파일을 Git에서 자동으로 가져오려면:

```yaml
# helm/airflow/values.yaml
dags:
  gitSync:
    enabled: true
    repo: https://github.com/your-org/your-repo.git
    branch: main
```

또는 PVC를 사용하여 DAG를 공유:

```yaml
dags:
  persistence:
    enabled: true
    size: 10Gi
```

## 접속 방법

### Airflow Webserver
```bash
# Port forwarding
kubectl port-forward -n airflow svc/airflow-webserver 8080:8080

# 브라우저 접속
# http://localhost:8080
# 기본 ID/PW: admin / admin
```

### Flower (Celery 모니터링)
```bash
# Port forwarding
kubectl port-forward -n airflow svc/airflow-flower 5555:5555

# 브라우저 접속
# http://localhost:5555
```

### PostgreSQL
```bash
# Port forwarding
kubectl port-forward -n airflow svc/postgresql 5432:5432

# psql 접속
psql -h localhost -U airflow -d airflow
# Password: airflow-password
```

### Redis
```bash
# Port forwarding
kubectl port-forward -n airflow svc/redis-master 6379:6379

# Redis CLI 접속
redis-cli -h localhost
```

## 트러블슈팅

### Airflow가 PostgreSQL에 연결되지 않음
```bash
# PostgreSQL 서비스 확인
kubectl get svc -n airflow postgresql

# Pod 상태 확인
kubectl get pods -n airflow
kubectl logs -n airflow -l app.kubernetes.io/name=airflow,component=scheduler
```

### Celery Worker가 시작되지 않음
```bash
# Redis 연결 확인
kubectl logs -n airflow -l app.kubernetes.io/name=airflow,component=worker

# Worker Pod 상태 확인
kubectl get pods -n airflow -l component=worker
```

### DAG가 보이지 않음
```bash
# DAG Pod 상태 확인
kubectl get pods -n airflow -l component=scheduler

# Git Sync 로그 확인
kubectl logs -n airflow -l component=scheduler -c git-sync
```

## 업그레이드

```bash
# ArgoCD를 통한 자동 업그레이드 (syncPolicy.automated 사용 시)
# 또는 수동으로 Helm 차트 업그레이드

helm upgrade airflow apache-airflow/airflow \
  --namespace airflow \
  -f helm/airflow/values.yaml
```

## 삭제

```bash
# ArgoCD Application 삭제
kubectl delete -f helm/argoapps/

# 또는 Helm 릴리스 삭제
helm uninstall airflow -n airflow
helm uninstall redis -n airflow
helm uninstall postgresql -n airflow
```

## 주요 특징

✅ **Celery Executor 사용**: 분산 작업 처리  
✅ **스케일링 가능**: Worker 수를 쉽게 조정  
✅ **모니터링**: Flower를 통한 Celery 작업 모니터링  
✅ **Git Sync**: DAG 자동 동기화  
✅ **고가용성**: PostgreSQL Read Replica 지원  
✅ **자동 동기화**: ArgoCD를 통한 자동 배포 및 자가 복구  
✅ **Cloud Native**: Kubernetes에 최적화

## 보안 고려사항

1. **FERNET_KEY**: 실제 환경에서는 안전한 Fernet Key를 생성하세요
2. **비밀번호**: helm values의 기본 비밀번호를 변경하세요
3. **NetworkPolicy**: 필요시 네트워크 정책을 추가하세요
4. **RBAC**: 적절한 서비스 어카운트와 역할을 설정하세요

