"""
function_converter 모듈 테스트
"""

import pytest
from unittest.mock import patch, MagicMock
from airflow_plugins.utils.function_converter import (
    function_to_script,
    extract_function_dependencies,
    create_function_wrapper
)


def sample_function():
    """테스트용 샘플 함수"""
    import os
    x = 1
    y = 2
    return x + y


def sample_function_with_imports():
    """Import를 사용하는 함수"""
    from datetime import datetime
    import json
    
    date = datetime.now()
    data = {"key": "value"}
    return json.dumps(data)


class TestFunctionToScript:
    """function_to_script 함수 테스트"""
    
    def test_basic_conversion(self):
        """기본 함수 변환 테스트"""
        script = function_to_script(sample_function)
        
        assert "def sample_function():" in script
        assert "if __name__ == \"__main__\":" in script
        assert "sample_function()" in script
    
    def test_without_main_block(self):
        """main 블록 없이 변환 테스트"""
        script = function_to_script(sample_function, add_main_block=False)
        
        assert "def sample_function():" in script
        assert "if __name__ == \"__main__\":" not in script
    
    def test_with_imports(self):
        """Import 포함 변환 테스트"""
        script = function_to_script(sample_function_with_imports, include_imports=True)
        
        assert "def sample_function_with_imports():" in script
        # 기본적으로 함수 안의 import는 포함되지 않음
        # inspect.getsource()가 함수 정의만 반환하기 때문
    
    def test_error_handling(self):
        """에러 처리 테스트"""
        # 잘못된 객체 전달
        with pytest.raises(Exception):
            function_to_script("not a function")


class TestExtractFunctionDependencies:
    """extract_function_dependencies 함수 테스트"""
    
    def test_extract_imports(self):
        """Import 문 추출 테스트"""
        dependencies = extract_function_dependencies(sample_function_with_imports)
        
        assert any("datetime" in dep for dep in dependencies)
        assert any("json" in dep for dep in dependencies)
    
    def test_no_imports(self):
        """Import 없는 함수 테스트"""
        def simple_func():
            x = 1
            return x
        
        dependencies = extract_function_dependencies(simple_func)
        assert len(dependencies) == 0


class TestCreateFunctionWrapper:
    """create_function_wrapper 함수 테스트"""
    
    def test_basic_wrapper(self):
        """기본 Wrapper 생성 테스트"""
        wrapper = create_function_wrapper(sample_function)
        
        assert "def main():" in wrapper
        assert "def sample_function():" in wrapper
        assert "if __name__ == \"__main__\":" in wrapper
        assert "sample_function()" in wrapper
    
    def test_custom_wrapper_name(self):
        """커스텀 Wrapper 이름 테스트"""
        wrapper = create_function_wrapper(sample_function, wrapper_name="custom_main")
        
        assert "def custom_main():" in wrapper
        assert "custom_main()" in wrapper
    
    def test_with_additional_code(self):
        """추가 코드 포함 Wrapper 생성 테스트"""
        additional = "        print('Additional code')"
        wrapper = create_function_wrapper(sample_function, additional_code=additional)
        
        assert "Additional code" in wrapper
        assert "print('Additional code')" in wrapper


class TestIntegration:
    """통합 테스트"""
    
    def test_function_to_script_with_wrapper(self):
        """function_to_script와 wrapper 함께 사용"""
        # 함수를 스크립트로 변환
        script = function_to_script(sample_function, add_main_block=True)
        
        assert "def sample_function():" in script
        assert "sample_function()" in script
        
        # Wrapper 생성
        wrapper = create_function_wrapper(sample_function)
        
        assert "def main():" in wrapper
        assert "sample_function()" in wrapper

