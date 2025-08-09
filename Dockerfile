# Python 3.11 기반 이미지 사용
FROM python:3.11-slim

# 작업 디렉토리 설정
WORKDIR /app

# 시스템 패키지 업데이트 및 필요한 도구 설치
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 파일 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 파일 복사
COPY main.py .
COPY kelly.py .

# 포트 8001 노출 (환경변수 PORT=8001 사용)
EXPOSE 8001

# 환경 변수 설정
ENV PYTHONUNBUFFERED=1

# 애플리케이션 실행
CMD ["python", "main.py"]