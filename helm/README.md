# Helm Charts for Airflow ETL Stack

Kubernetes 클러스터에 Airflow, PostgreSQL, Redis를 배포하는 Helm 차트 및 ArgoCD Application 매니페스트입니다.

## 폴더 구조

```
helm/
├── argoapps/              # ArgoCD Application 매니페스트
│   ├── postgresql-app.yaml
│   ├── redis-app.yaml
│   ├── airflow-app.yaml
│   └── kustomization.yaml
├── postgresql/           # PostgreSQL Helm values
│   └── values.yaml
├── redis/               # Redis Helm values
│   └── values.yaml
├── airflow/            # Airflow Helm values
│   └── values.yaml
├── README.md
└── .gitignore
```

## 구성 요소

### 1. PostgreSQL (Cloud Native PostgreSQL)
- **Chart**: Bitnami PostgreSQL
- **Chart Version**: 15.1.2
- **용도**: Airflow 메타데이터 데이터베이스
- **네임스페이스**: airflow
- **리소스**: 
  - Storage: 20Gi
  - Memory: 512Mi - 1Gi
  - CPU: 250m - 500m

**설정:**
- User: airflow
- Password: airflow-password
- Database: airflow
- PostgreSQL Password: airflow-postgres

### 2. Redis (Standalone Redis)
- **Chart**: Bitnami Redis
- **Chart Version**: 19.1.0
- **용도**: Celery 메시지 브로커
- **아키텍처**: Standalone (Sentry Redis 대신)
- **네임스페이스**: airflow
- **리소스**:
  - Storage: 8Gi
  - Memory: 256Mi - 512Mi
  - CPU: 100m - 200m

**설정:**
- Auth: Disabled
- Metrics: Disabled

### 3. Apache Airflow
- **Chart**: Apache Airflow
- **Chart Version**: 1.13.1
- **Airflow Version**: 3.0.6
- **Executor**: CeleryExecutor
- **네임스페이스**: airflow
- **Git Sync**: https://github.com/taengkim/etl-to-iceberg.git

**컴포넌트:**
- **Webserver** (1 replica)
  - Memory: 512Mi - 1Gi
  - CPU: 250m - 500m
- **Scheduler** (1 replica)
  - Memory: 512Mi - 1Gi
  - CPU: 250m - 500m
- **Celery Workers** (2 replicas)
  - Memory: 1Gi - 2Gi
  - CPU: 500m - 1000m
- **Flower** (1 replica)
  - Memory: 128Mi - 256Mi
  - CPU: 100m - 200m

**연결 정보:**
- PostgreSQL Host: `postgresql.airflow.svc.cluster.local:5432`
- Redis Host: `redis-master.airflow.svc.cluster.local:6379`

---

## 배포 방법

### ArgoCD를 통한 자동 배포 (권장)

#### 1. ArgoCD Repository 추가

먼저 Helm chart 저장소를 ArgoCD에 추가:

```bash
# Bitnami 저장소
argocd repo add https://charts.bitnami.com/bitnami \
    --type helm \
    --name bitnami

# Apache Airflow 저장소
argocd repo add https://airflow.apache.org \
    --type helm \
    --name airflow
```

#### 2. ArgoCD Application 배포

```bash
# 모든 Application 한 번에 배포
kubectl apply -k helm/argoapps/

# 또는 개별 배포
kubectl apply -f helm/argoapps/postgresql-app.yaml
kubectl apply -f helm/argoapps/redis-app.yaml
kubectl apply -f helm/argoapps/airflow-app.yaml
```

#### 3. 배포 상태 확인

```bash
# Application 상태 확인
kubectl get applications -n argocd

# Pod 상태 확인
kubectl get pods -n airflow

# 모든 서비스 확인
kubectl get svc -n airflow
```

---

### Helm CLI를 통한 수동 배포

#### 1. Repository 추가

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

#### 2. PostgreSQL 설치

```bash
helm install postgresql bitnami/postgresql \
    --namespace airflow \
    --create-namespace \
    -f helm/postgresql/values.yaml
```

#### 3. Redis 설치

```bash
helm install redis bitnami/redis \
    --namespace airflow \
    -f helm/redis/values.yaml
```

#### 4. Airflow 설치

PostgreSQL과 Redis가 준비될 때까지 대기:

```bash
# PostgreSQL 준비 확인
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=postgresql -n airflow --timeout=300s

# Redis 준비 확인
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=redis -n airflow --timeout=300s

# Airflow 설치
helm install airflow apache-airflow/airflow \
    --namespace airflow \
    -f helm/airflow/values.yaml
```

---

## 설정 커스터마이징

### PostgreSQL 설정 변경

`helm/postgresql/values.yaml`:

```yaml
auth:
  postgresPassword: <새로운 비밀번호>
  username: airflow
  password: <새로운 비밀번호>

persistence:
  size: 50Gi  # Storage 크기 증가
```

### Redis 설정 변경

`helm/redis/values.yaml`:

```yaml
auth:
  enabled: true
  password: <비밀번호 추가>

master:
  persistence:
    size: 16Gi  # Storage 크기 증가
```

### Airflow 리소스 조정

`helm/airflow/values.yaml`:

```yaml
webserver:
  replicas: 2  # Webserver 수 증가
  resources:
    limits:
      memory: "2Gi"

workers:
  replicas: 5  # Worker 수 증가
  resources:
    limits:
      memory: "4Gi"
```

### Git Sync Repository 변경

`helm/airflow/values.yaml`:

```yaml
dags:
  gitSync:
    repo: https://github.com/your-org/your-repo.git  # 실제 저장소
    branch: main
```

---

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

---

## 업그레이드

### Helm 차트 업그레이드

```bash
# PostgreSQL
helm upgrade postgresql bitnami/postgresql \
    --namespace airflow \
    -f helm/postgresql/values.yaml

# Redis
helm upgrade redis bitnami/redis \
    --namespace airflow \
    -f helm/redis/values.yaml

# Airflow
helm upgrade airflow apache-airflow/airflow \
    --namespace airflow \
    -f helm/airflow/values.yaml
```

### ArgoCD 자동 업그레이드

`syncPolicy.automated`가 활성화되어 있으면 자동으로 업그레이드됩니다.

---

## 삭제

### ArgoCD를 통한 삭제

```bash
kubectl delete -f helm/argoapps/
```

### Helm을 통한 삭제

```bash
helm uninstall airflow -n airflow
helm uninstall redis -n airflow
helm uninstall postgresql -n airflow
```

---

## 트러블슈팅

### Airflow가 PostgreSQL에 연결되지 않음

```bash
# PostgreSQL 서비스 확인
kubectl get svc -n airflow postgresql

# Pod 상태 확인
kubectl get pods -n airflow

# Airflow Scheduler 로그 확인
kubectl logs -n airflow -l component=scheduler -f
```

### Celery Worker가 시작되지 않음

```bash
# Redis 연결 확인
kubectl get svc -n airflow redis-master

# Worker 로그 확인
kubectl logs -n airflow -l component=worker -f

# Flower에서 Worker 상태 확인
# http://localhost:5555
```

### DAG가 보이지 않음

```bash
# Git Sync 로그 확인
kubectl logs -n airflow -l component=scheduler -c git-sync

# DAG 폴더 확인
kubectl exec -it -n airflow deploy/airflow-webserver -- ls -la /opt/airflow/dags
```

### 메모리 부족

리소스를 조정하세요:

```yaml
# helm/airflow/values.yaml
webserver:
  resources:
    requests:
      memory: "1Gi"
    limits:
      memory: "2Gi"
```

---

## 주요 특징

✅ **Celery Executor**: 분산 작업 처리  
✅ **Auto Scaling**: Worker 수를 쉽게 조정  
✅ **Monitoring**: Flower를 통한 Celery 작업 모니터링  
✅ **Git Sync**: DAG 자동 동기화  
✅ **High Availability**: PostgreSQL Read Replica 지원  
✅ **Auto Sync**: ArgoCD를 통한 자동 배포 및 자가 복구  
✅ **Cloud Native**: Kubernetes에 최적화  
✅ **Airflow 3.0.6**: 최신 Airflow 버전 사용

---

## 보안 고려사항

1. **비밀번호**: 모든 기본 비밀번호를 변경하세요
2. **FERNET_KEY**: 안전한 Fernet Key를 생성하세요
3. **NetworkPolicy**: 필요시 네트워크 정책 추가
4. **RBAC**: 적절한 서비스 어카운트와 역할 설정
5. **Git Repository**: Private 저장소 사용 권장

---

## 참고 자료

- [Apache Airflow Helm Chart](https://github.com/apache/airflow/tree/main/chart)
- [Bitnami PostgreSQL Chart](https://github.com/bitnami/charts/tree/main/bitnami/postgresql)
- [Bitnami Redis Chart](https://github.com/bitnami/charts/tree/main/bitnami/redis)
- [ArgoCD Documentation](https://argo-cd.readthedocs.io/)
