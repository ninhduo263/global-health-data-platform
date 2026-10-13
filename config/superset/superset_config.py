import os

# Khóa bảo mật bắt buộc cho Apache Superset
SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "covid19_public_health_data_platform_secret_key_2026"
)

# Bật tính năng upload CSV và Template Processing nếu cần cho Dashboard Y tế
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Cho phép nhúng hoặc kết nối ổn định trong môi trường Docker local
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False

