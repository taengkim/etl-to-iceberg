"""
함수 변환 유틸리티

Python 함수를 소스 코드 문자열로 변환하는 공통 함수들입니다.
"""

import inspect
from typing import Callable, Optional
from airflow.exceptions import AirflowException


def function_to_script(
    func: Callable,
    include_imports: bool = False,
    add_main_block: bool = True
) -> str:
    """
    Python 함수를 소스 코드 문자열로 변환
    
    :param func: Python 함수
    :param include_imports: 함수에서 사용하는 import문도 포함할지 여부
    :param add_main_block: if __name__ == "__main__" 블록 추가 여부
    :return: 소스 코드 문자열
    """
    try:
        # 함수의 소스 코드 가져오기
        source = inspect.getsource(func)
        
        # 함수 이름
        func_name = func.__name__
        
        # Import문 가져오기 (선택사항)
        imports = ''
        if include_imports:
            # 함수에서 사용하는 import 추출 시도
            try:
                module = inspect.getmodule(func)
                if module:
                    # 모듈의 모든 import 추출
                    module_source = inspect.getsource(module)
                    import_lines = [line for line in module_source.split('\n') 
                                  if line.strip().startswith('import ') or line.strip().startswith('from ')]
                    imports = '\n'.join(import_lines) + '\n' if import_lines else ''
            except:
                pass
        
        # 완전한 실행 가능한 스크립트 생성
        script = imports + source
        
        # 메인 실행 블록 추가
        if add_main_block:
            script += f"""

# 함수 실행
if __name__ == "__main__":
    {func_name}()
"""
        
        return script
    
    except Exception as e:
        raise AirflowException(f"함수를 소스 코드로 변환 실패: {e}")


def extract_function_dependencies(func: Callable) -> List[str]:
    """
    함수에서 사용하는 모듈 의존성 추출
    
    :param func: Python 함수
    :return: 사용하는 모듈 리스트
    """
    try:
        # 함수의 소스 코드 가져오기
        source = inspect.getsource(func)
        
        # import 문 찾기
        import_lines = []
        for line in source.split('\n'):
            stripped = line.strip()
            if stripped.startswith('import ') or stripped.startswith('from '):
                import_lines.append(stripped)
        
        return import_lines
    
    except Exception as e:
        return []


def create_function_wrapper(
    func: Callable,
    wrapper_name: str = 'main',
    additional_code: Optional[str] = None
) -> str:
    """
    함수를 감싸는 Wrapper 스크립트 생성
    
    :param func: Python 함수
    :param wrapper_name: Wrapper 함수 이름
    :param additional_code: 추가 코드 (선택사항)
    :return: Wrapper 스크립트 문자열
    """
    try:
        # 함수의 소스 코드 가져오기
        source = inspect.getsource(func)
        func_name = func.__name__
        
        # Wrapper 함수 생성
        wrapper = f"""
{source}

def {wrapper_name}():
    \"\"\"Wrapper function to execute {func_name}\"\"\"
    try:
        # 추가 코드 실행
{additional_code or ''}
        
        # 원본 함수 실행
        {func_name}()
    
    except Exception as e:
        print(f"Error in {func_name}: {{e}}")
        raise

if __name__ == "__main__":
    {wrapper_name}()
"""
        
        return wrapper
    
    except Exception as e:
        raise AirflowException(f"함수 Wrapper 생성 실패: {e}")

