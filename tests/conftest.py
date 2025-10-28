"""
pytest 설정 파일
"""

import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Airflow 관련 모듈 설정
import os
os.environ['AIRFLOW_HOME'] = str(project_root / 'airflow_home')

