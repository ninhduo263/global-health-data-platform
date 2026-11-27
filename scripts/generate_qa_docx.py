# -*- coding: utf-8 -*-
"""
Sinh tài liệu Word (.docx) chứa Bộ 28 Câu hỏi Phản biện & Đáp án Chi tiết
phục vụ bảo vệ Đồ án Học phần Kho dữ liệu & Khai phá dữ liệu (INT1422 - PTIT).

Đầu ra mặc định: baocao/BO_CAU_HOI_BAO_VE_DO_AN.docx
"""

from pathlib import Path

from docx import Document
from docx.enum.table import WD_ALIGN_VERTICAL, WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import nsdecls, qn
from docx.shared import Cm, Pt, RGBColor

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "baocao" / "BO_CAU_HOI_BAO_VE_DO_AN.docx"

NAVY = RGBColor(0x1E, 0x3A, 0x8A)
TEAL = RGBColor(0x0F, 0x76, 0x6E)
DARK = RGBColor(0x0F, 0x17, 0x2A)
SLATE = RGBColor(0x33, 0x41, 0x55)
MUTED = RGBColor(0x64, 0x74, 0x8B)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)


# ==============================================================================
# DỮ LIỆU 28 CÂU HỎI & ĐÁP ÁN BẢO VỆ CHUYÊN SÂU (CHIA THEO 6 NHÓM CHỦ ĐỀ)
# ==============================================================================
QA_SECTIONS = [
    {
        "section_title": "PHẦN 1: KIẾN TRÚC DATA PLATFORM, BATCH ETL & KHO DỮ LIỆU SNOWFLAKE (CÂU 1 – CÂU 5)",
        "items": [
            {
                "q": "Câu 1: Hai luồng Batch ETL (Apache Airflow) và Streaming CDC (Debezium – Kafka) cùng đổ vào chung một Kho dữ liệu PostgreSQL thì có xảy ra xung đột (conflict / ghi đè / deadlock) không? Nếu không thì cơ chế nào ngăn chặn?",
                "short": "Không xảy ra xung đột ghi đè hay deadlock. Hệ thống ngăn chặn bằng nguyên tắc Phân tách chủ quyền ghi theo bảng (Single-Writer Principle): Airflow chỉ ghi bảng dịch tễ vĩ mô toàn cầu và kết quả ML, còn CDC độc quyền ghi các bảng bệnh nhân/tiêm chủng nội bộ. Bảng duy nhất cả hai cùng chạm tới là bảng lịch dwh.dim_date được xử lý bằng khóa tự nhiên tất định YYYYMMDD kèm lệnh INSERT ... ON CONFLICT (date_id) DO NOTHING, kết hợp cơ chế MVCC của PostgreSQL giúp Đọc – Ghi đồng thời không khóa nhau.",
                "points": [
                    ("Phân tách chủ quyền ghi tuyệt đối (Single-Writer Ownership): ", "Trong airflow/dags/health_data_pipeline.py, task extract_mysql_oltp chỉ xuất bản chụp CSV lưu vào MinIO Data Lake (raw/oltp/{ds}/) để đối soát kiểm toán chứ KHÔNG ghi bảng OLTP vào PostgreSQL DWH. Trong DWH, Airflow chỉ ghi dim_region, dim_country, dim_indicator, fact_cases, fact_who_covid_daily; ngược lại cdc/consumer.py độc quyền ghi dim_vaccination_center, dim_patient, fact_vaccination và fact_realtime_anomalies."),
                    ("Tách khóa ngoại chéo miền: ", "Cột country_code ở dim_patient và dim_vaccination_center lưu mã VARCHAR(10) độc lập thay vì khóa ngoại cứng sang dim_country(id), nên khi Airflow làm mới dim_country và fact_cases sẽ không bao giờ vướng ràng buộc khóa ngoại hay giữ khóa bảng lên luồng CDC."),
                    ("Xử lý bảng dùng chung duy nhất (dwh.dim_date): ", "Không dùng khóa tự tăng SERIAL (dễ lệch mã giữa 2 luồng) mà dùng khóa tự nhiên tất định date_id = YYYYMMDD (sinh sẵn giai đoạn 2020–2025 trong init-dwh.sql), không bao giờ DELETE/TRUNCATE dim_date và cả hai luồng đều dùng INSERT ... ON CONFLICT (date_id) DO NOTHING."),
                    ("Đọc – Ghi đồng thời không chặn nhau nhờ PostgreSQL MVCC: ", "Khi task patient_association_rules (Apriori trên Airflow) hoặc Superset thực thi SELECT trên dim_patient và fact_vaccination đúng lúc cdc-consumer đang INSERT/UPDATE liên tục, cơ chế Snapshot Isolation (READ COMMITTED) của PostgreSQL đảm bảo luồng đọc lấy bản chụp nhất quán mà không chặn luồng ghi."),
                    ("Chống chồng chéo tiến trình & Giao dịch nguyên tử: ", "Airflow cấu hình max_active_runs=1 và gói trọn bước nạp trong 1 transaction (chỉ commit ở cuối); CDC tắt auto-commit offset (enable_auto_commit=False), chỉ commit Kafka sau khi commit PostgreSQL thành công."),
                ],
            },
            {
                "q": "Câu 2: Tại sao nhóm lựa chọn mô hình Bông tuyết (Snowflake Schema) thay vì mô hình Hình sao (Star Schema) truyền thống cho Kho dữ liệu của đề tài?",
                "short": "Nhóm chọn Snowflake Schema để chuẩn hóa phân cấp địa lý quốc tế (dim_country tham chiếu sang dim_region theo châu lục/vùng WHO), giúp loại bỏ dư thừa dữ liệu và đảm bảo tính nhất quán khi cập nhật hơn 200 quốc gia, đồng thời khắc phục nhược điểm tốn phép JOIN của Snowflake bằng hệ thống 17 Semantic Views và đánh chỉ mục B-Tree trên toàn bộ khóa ngoại.",
                "points": [
                    ("Chuẩn hóa chiều phân cấp địa lý: ", "Dữ liệu dịch tễ toàn cầu có cấu trúc phân cấp tự nhiên Khu vực/Châu lục (dim_region) → Quốc gia (dim_country) → Sự kiện ngày (fact_cases). Tách dim_region tránh lặp lại chuỗi tên châu lục/vùng WHO hàng trăm lần và ngăn dị thường cập nhật (Update Anomaly)."),
                    ("Kết hợp linh hoạt với chiều vận hành CDC: ", "Nhánh tiêm chủng nội bộ cũng tuân theo mô hình chiều chuẩn hóa gồm dim_patient, dim_vaccination_center và dim_date nối vào fact_vaccination."),
                    ("Đảm bảo hiệu năng truy vấn OLAP: ", "Với quy mô ~395.000 dòng fact_cases, việc thêm 1 phép LEFT JOIN từ dim_country sang dim_region (chỉ gồm 6 dòng châu lục nằm gọn trong bộ nhớ đệm RAM) tốn chưa tới 1 mili-giây khi đã có Index trên country_id và date_id, trong khi toàn bộ biểu đồ Superset đọc qua các View dwh.vw_* đã định nghĩa sẵn phép nối."),
                ],
            },
            {
                "q": "Câu 3: Đã có Kho dữ liệu PostgreSQL rồi, tại sao hệ thống phải dựng thêm tầng MinIO Data Lake (Landing Zone) ở giữa làm gì cho phức tạp và tốn tài nguyên?",
                "short": "MinIO Data Lake đóng vai trò vùng hạ cánh dữ liệu thô bất biến (Immutable Raw Landing Zone) phân vùng theo ngày chạy, giúp tách biệt bước Thu thập (Extract) khỏi bước Biến đổi (Transform), cho phép chạy lại (Replay/Backfill) khi đổi logic làm sạch mà không phụ thuộc mạng hay API bên ngoài, đồng thời lưu bằng chứng kiểm toán dữ liệu gốc.",
                "points": [
                    ("Khả năng tái lập & chạy lại độc lập (Reproducibility): ", "File CSV của OWID nặng hàng chục MB tải qua mạng từ GitHub; nếu bước chuẩn hóa SQL hoặc ép kiểu gặp lỗi, hoặc khi nhóm muốn bổ sung thêm cột đặc trưng mới từ OWID (như people_fully_vaccinated cho XGBoost), Airflow đọc lại trực tiếp từ bucket s3://covid19-landing-zone/raw/owid/{ds}/ trong mạng nội bộ tốc độ cao mà không cần tải lại từ Internet."),
                    ("Bảo toàn dữ liệu gốc trước khi làm sạch: ", "Dữ liệu thực tế từ OWID/WHO có các giá trị rỗng, ngoại lai hoặc số âm hiệu chỉnh hồi tố. Lưu bản gốc phân vùng theo ngày (raw/owid/, raw/who/, raw/oltp/) giúp đối chiếu chênh lệch giữa nguồn và DWH bất kỳ lúc nào."),
                    ("Chuẩn S3 tương thích đám mây: ", "Sử dụng thư viện boto3 chuẩn S3 giúp mã nguồn có thể chuyển dịch thẳng từ MinIO cục bộ lên AWS S3 / Google Cloud Storage mà không phải sửa đổi logic code."),
                ],
            },
            {
                "q": "Câu 4: Cả nguồn OWID và nguồn WHO đều thống kê ca mắc và tử vong theo ngày. Tại sao nhóm không gộp chung vào một bảng fact_cases mà lại tách riêng bảng dwh.fact_who_covid_daily?",
                "short": "Hai nguồn khác nhau về chuẩn mã quốc gia (OWID dùng ISO-3 như VNM/USA, bộ WHO dùng ISO-2 như VN/US), khác quy ước giờ chốt báo cáo ngày và khác phạm vi thuộc tính (OWID có dân số, vắc-xin, giường bệnh; WHO chuẩn hóa theo khu vực dịch tễ WHO Region). Tách bảng giúp bảo toàn đúng độ mịn (grain) và tránh nhân đôi số ca hoặc join sai mã.",
                "points": [
                    ("Ngăn chặn lỗi nhân đôi số liệu (Double Counting): ", "Nếu chèn chung cả dòng của OWID và WHO cho cùng Việt Nam trong cùng 1 ngày vào fact_cases thì hàm SUM(new_cases) trên Dashboard sẽ bị gấp đôi sai bản chất."),
                    ("Tôn trọng chuẩn định danh của từng tổ chức: ", "fact_cases liên kết chặt với dim_country (mã ISO-3, hơn 200 quốc gia) làm đầu vào xuyên suốt cho K-Means, Markov, XGBoost và Isolation Forest; trong khi fact_who_covid_daily giữ nguyên cấu trúc chuẩn của Tổ chức Y tế Thế giới cho 15 quốc gia trọng điểm để đối chiếu chéo."),
                ],
            },
            {
                "q": "Câu 5: Nếu pipeline Airflow đang chạy làm mới bảng fact_cases mà người dùng bấm xem Dashboard Superset, họ có nhìn thấy bảng trống 0 dòng không? Nếu chạy lại DAG nhiều lần trong ngày có bị trùng dữ liệu không?",
                "short": "Người dùng KHÔNG BAO GIỜ thấy bảng trống 0 dòng và dữ liệu KHÔNG BAO GIỜ bị nhân đôi. Toàn bộ quá trình làm mới và nạp lô (execute_values) nằm trọn trong một Transaction ACID duy nhất của PostgreSQL, đồng thời mọi bảng đích của ETL và 5 mô hình ML đều dùng khóa chính nghiệp vụ kèm mệnh đề ON CONFLICT DO UPDATE.",
                "points": [
                    ("Tính nguyên tử của Transaction (All-or-Nothing): ", "Trong transform_and_load_to_dwh_callable và cả 5 module Data Mining, lệnh xóa dữ liệu phiên bản cũ và chèn lô dữ liệu mới đều dùng chung một đối tượng kết nối conn chưa commit. Nhờ MVCC, mọi truy vấn từ Superset hay AI Portal vẫn đọc bản dữ liệu cũ ổn định cho tới đúng mili-giây lệnh conn.commit() hoàn tất; nếu có ngoại lệ giữa chừng, conn.rollback() khôi phục nguyên trạng 100%."),
                    ("Tính lũy đẳng (Idempotency) toàn phần: ", "Các khóa ràng buộc như UNIQUE (country_id, date_id) trên fact_cases, PRIMARY KEY (week_start, country_code, model_version) trên bảng phân cụm/cảnh báo sớm/bất thường đảm bảo chạy lại bao nhiêu lần kết quả vẫn duy nhất và nhất quán."),
                ],
            },
        ],
    },
    {
        "section_title": "PHẦN 2: ĐỒNG BỘ THỜI GIAN THỰC LOG-BASED CDC, KAFKA & BẢO MẬT PII (CÂU 6 – CÂU 10)",
        "items": [
            {
                "q": "Câu 6: Tại sao nhóm phải triển khai Debezium + Apache Kafka phức tạp cho luồng OLTP thay vì dùng lệnh SQL định kỳ (Query-based Polling: SELECT ... WHERE updated_at > last_run) mỗi vài phút?",
                "short": "Cơ chế Log-based CDC đọc trực tiếp tệp nhật ký nhị phân (Binlog chế độ ROW) của MySQL nên có 3 ưu thế tuyệt đối so với Query-based Polling: (1) độ trễ dưới 2 giây thay vì chờ chu kỳ phút/giờ, (2) bắt được cả thao tác xóa cứng DELETE mà Polling bị mù, và (3) không đánh truy vấn quét bảng làm giảm hiệu năng CSDL giao dịch OLTP.",
                "points": [
                    ("Bắt trọn vẹn mọi biến động hàng (INSERT / UPDATE / DELETE): ", "Khi một bản ghi tiêm chủng hoặc bệnh nhân bị xóa trong MySQL, dòng đó biến mất khỏi bảng nên SELECT WHERE updated_at > ... không thể biết; ngược lại Binlog ghi lại sự kiện op='d' kèm khóa chính trước khi xóa (before), giúp cdc/consumer.py xóa đồng bộ ngay trong DWH."),
                    ("Chịu tải cao nhờ hàng đợi phân tán Kafka: ", "Khi các điểm tiêm đồng loạt nhập hàng nghìn mũi tiêm cùng lúc (như kịch bản seed_oltp_data.py hoặc demo_realtime_outbreak.ps1), Kafka đóng vai trò bộ đệm (backpressure buffer) giữ an toàn toàn bộ sự kiện để consumer xử lý tuần tự mà không làm quá tải PostgreSQL DWH."),
                ],
            },
            {
                "q": "Câu 7: Ba bảng OLTP đi qua 3 Kafka topics độc lập, vậy nếu bản tin mũi tiêm (vaccination_records) đến Consumer trước bản tin hồ sơ bệnh nhân (patients) thì có bị lỗi khóa ngoại Foreign Key không?",
                "short": "Không bị lỗi nhờ kỹ thuật Placeholder Dimension (bản ghi chiều giữ chỗ sớm). Khi mũi tiêm đến trước mà chưa có patient_id hoặc center_id trong DWH, Consumer tự động tạo trước một dòng tạm với cờ is_placeholder = TRUE để thỏa mãn khóa ngoại; khi sự kiện bệnh nhân thật đến sau sẽ ghi đè đầy đủ thuộc tính và chuyển is_placeholder = FALSE.",
                "points": [
                    ("Nguyên nhân thực tế trên hệ phân tán: ", "Kafka chỉ đảm bảo thứ tự thông điệp bên trong cùng 1 partition của cùng 1 topic, hoàn toàn KHÔNG đảm bảo thứ tự thời gian giữa topic health_cdc.vaccination_oltp.patients và health_cdc.vaccination_oltp.vaccination_records."),
                    ("Cài đặt cụ thể trong cdc/consumer.py (dòng 392–437) & kiểm chứng Unit Test: ", "Hàm apply_record_event chạy INSERT INTO dwh.dim_patient (patient_id, is_placeholder) VALUES (%s, TRUE) ON CONFLICT (patient_id) DO NOTHING trước khi chèn fact_vaccination. Kịch bản này đã được kiểm thử tự động trong cdc/test_consumer.py (11/11 PASS)."),
                ],
            },
            {
                "q": "Câu 8: Nhóm xử lý bài toán Bảo mật dữ liệu cá nhân (Privacy by Design) như thế nào? Nếu đã xóa họ tên và CCCD thì làm sao biết bệnh nhân nào tiêm mấy mũi để chạy thuật toán Apriori?",
                "short": "Hệ thống áp dụng kết hợp Cắt bỏ định danh trực tiếp (Data Minimization: loại bỏ hoàn toàn full_name và citizen_hash trước khi vào DWH) và Giả danh hóa bằng khóa thay thế nội bộ (Pseudonymization qua khóa số nguyên patient_id), nhờ đó vừa không thể truy ngược danh tính công dân trong DWH vừa nối chính xác lịch sử mũi tiêm của từng cá thể.",
                "points": [
                    ("Chặn PII ở cả 2 cửa ngõ: ", "Ở luồng Batch backup lên MinIO, extract_mysql_oltp_callable lọc bỏ cột full_name ngay từ truy vấn INFORMATION_SCHEMA. Ở luồng CDC vào DWH, apply_patient_event hoàn toàn không đọc và bảng dwh.dim_patient không hề có cột full_name hay citizen_hash."),
                    ("Giữ nguyên giá trị phân tích dịch tễ: ", "Các thuộc tính không định danh gồm birth_year (quy đổi nhóm tuổi), gender, province, health_status và comorbidity kết hợp khóa nối patient_id sang fact_vaccination là vừa đủ 100% để thống kê BI và khai phá luật kết hợp Apriori."),
                ],
            },
            {
                "q": "Câu 9: Nếu container cdc-consumer bị sập nguồn đột ngột ngay khi đang xử lý dở một sự kiện Kafka thì khi bật lại có bị mất dữ liệu hoặc nhân đôi mũi tiêm không?",
                "short": "Không mất và không nhân đôi nhờ cơ chế Quản lý Offset thủ công gắn liền giao dịch ACID (Transactional Sink + Manual Offset Commit) kết hợp câu lệnh ghi đè lũy đẳng UPSERT.",
                "points": [
                    ("Không bao giờ mất sự kiện (At-least-once delivery): ", "KafkaConsumer đặt enable_auto_commit=False. Chỉ sau khi connection.commit() ghi thành công vào PostgreSQL thì dòng tiếp theo mới gọi consumer.commit() lưu offset lên Kafka. Nếu lỗi SQL, khối except gọi connection.rollback() và không tăng offset."),
                    ("Không bao giờ nhân đôi bản ghi (Idempotent Upsert): ", "Trường hợp hi hữu nhất là PostgreSQL vừa commit xong nhưng tiến trình sập trước mili-giây gọi consumer.commit() — khi sống dậy consumer đọc lại đúng bản tin đó và lệnh ON CONFLICT (record_id / patient_id) DO UPDATE chỉ cập nhật lại đúng giá trị cũ chứ không sinh dòng thứ hai."),
                ],
            },
            {
                "q": "Câu 10: Vì sao trong hệ thống lại đặt tên connector là vaccination-oltp-connector-v2 và có hàm remove_legacy_connectors() tự động xóa connector v1?",
                "short": "Do cơ chế lưu offset của Debezium: khi mở rộng danh sách bảng cần bắt (thêm bảng patients vào connector v1 vốn chỉ bắt 2 bảng và đã chạy xong snapshot ban đầu), Debezium sẽ không tự chụp lại dữ liệu lịch sử của bảng mới thêm. Đăng ký connector v2 với tên & lịch sử schema mới giúp tự động snapshot trọn vẹn cả 3 bảng, còn hàm xóa v1 ngăn chặn 2 connector chạy song song đẩy trùng sự kiện.",
                "points": [
                    ("Giải quyết triệt để vấn đề tiến hóa lược đồ CDC (Schema Evolution): ", "Được tự động hóa hoàn toàn ngay trong hàm ensure_connector() của cdc/consumer.py, giúp giảng viên hoặc người triển khai nâng cấp từ bản cũ lên bản mới bằng một lệnh docker compose up -d duy nhất mà không cần xóa thủ công Kafka volume."),
                ],
            },
        ],
    },
    {
        "section_title": "PHẦN 3: PHÂN CỤM K-MEANS NÂNG CAO, PCA & CHUỖI CHUYỂN DỊCH MARKOV (CÂU 11 – CÂU 16)",
        "items": [
            {
                "q": "Câu 11: Tại sao nhóm không đưa thẳng số ca mắc mới và số tử vong tuyệt đối trong tuần vào thuật toán K-Means mà phải qua 3 bước: chuẩn hóa theo 100.000 dân, biến đổi log1p và StandardScaler?",
                "short": "Vì K-Means dùng khoảng cách Euclid rất nhạy cảm với quy mô dân số, độ lệch phân phối và đơn vị đo. Ba bước tiền xử lý giúp: (1) loại bỏ sai lệch quy mô dân số giữa nước lớn và nước nhỏ, (2) nén phân phối lệch phải cực đoan (heavy right-skew) của các đợt siêu bùng phát bằng ln(1+x), và (3) cân bằng trọng số giữa 3 đặc trưng bằng Z-score.",
                "points": [
                    ("Chuẩn hóa dân số (/100.000 dân): ", "Nếu dùng số tuyệt đối, 10.000 ca/tuần ở Ấn Độ (1,4 tỷ dân — mức rất thấp) sẽ bị coi giống 10.000 ca/tuần ở quốc gia 1 triệu dân (mức khủng hoảng)."),
                    ("Biến đổi log1p = ln(1 + x): ", "Dữ liệu dịch bệnh tuân theo quy luật hàm mũ: đa số tuần xấp xỉ 0–10 ca/100k nhưng đỉnh Omicron vọt lên >2.000 ca/100k. Nếu không lấy log1p, vài trăm điểm cực đại sẽ kéo lệch hoàn toàn tâm cụm và dồn 95% dữ liệu còn lại vào 1 cụm duy nhất. Dùng log1p thay vì log(x) để xử lý an toàn các tuần có 0 ca mắc/tử vong (ln(1+0) = 0)."),
                    ("Chuẩn hóa Z-score (StandardScaler): ", "Đưa cả 3 chiều (mức ca mắc log, mức tử vong log và chênh lệch log tuần) về trung bình 0, độ lệch chuẩn 1 để khoảng cách Euclid phản ánh công bằng cả mức độ nặng lẫn tốc độ tăng."),
                ],
            },
            {
                "q": "Câu 12: Trong bảng đánh giá số cụm K = 2..8, K = 5 có hệ số Silhouette cao nhất (0,5821), tại sao cấu hình mặc định nhóm lại chọn K = 4 (Silhouette = 0,5478)?",
                "short": "Trong Khai phá dữ liệu ứng dụng, việc chọn K phải cân bằng giữa Chỉ số toán học (Statistical Validity) và Khả năng diễn giải nghiệp vụ (Domain Interpretability). Tại K = 4, phương sai nội cụm Inertia đã qua điểm gãy khuỷu tay rõ rệt, Silhouette đạt mức tốt (> 0,54) và ánh xạ hoàn hảo vào đúng 4 cấp độ nguy cơ dịch tễ chuẩn của ngành y tế (Xanh – Vàng – Cam – Đỏ).",
                "points": [
                    ("So sánh thực nghiệm K = 4 và K = 5: ", "Khi tách lên K = 5, thuật toán chỉ chia nhỏ thêm nhóm mức dịch trung bình thành hai phân nhóm sát nhau, làm ma trận chuyển dịch Markov tăng từ 16 ô (4x4) lên 25 ô (5x5) gây phân mảnh nhận định mà không tạo ra khuyến nghị chính sách mới."),
                    ("Thiết kế linh hoạt bằng tham số cấu hình: ", "Hệ thống lưu đầy đủ kết quả của cả 7 giá trị K = 2..8 trong dwh.fact_kmeans_evaluation (đánh dấu cả is_selected cho K=4 và is_best_silhouette cho K=5) và hỗ trợ chế độ COVID_KMEANS_CLUSTERS=auto nếu người vận hành muốn chọn thuần theo đỉnh Silhouette."),
                ],
            },
            {
                "q": "Câu 13: Hệ số Silhouette có độ phức tạp O(N²). Với N = 56.477 quan sát và phải chạy lặp cho 7 giá trị K = 2..8, nhóm làm sao để Airflow không bị tràn RAM hay treo hàng chục phút?",
                "short": "Nhóm kết hợp lấy mẫu ngẫu nhiên cố định hạt giống (sample_size = 10.000, random_state = 42) riêng cho phép tính khoảng cách cặp của Silhouette, trong khi vẫn huấn luyện K-Means và tính Inertia, Davies–Bouldin, Calinski–Harabasz có độ phức tạp tuyến tính O(N·K) trên toàn bộ 100% tập 56.477 quan sát.",
                "points": [
                    ("Phân tích bài toán bộ nhớ: ", "Ma trận khoảng cách đầy đủ 56.477 × 56.477 kiểu float64 cần ~25,5 GB RAM cho mỗi lần gọi silhouette_score. Với cỡ mẫu 10.000 điểm đại diện phân phối toàn cục, sai số ước lượng Silhouette nhỏ hơn 0,002 nhưng bộ nhớ giảm hơn 30 lần và toàn bộ quá trình kiểm định K = 2..8 hoàn tất chỉ trong vài giây."),
                ],
            },
            {
                "q": "Câu 14: K-Means khởi tạo tâm cụm có yếu tố ngẫu nhiên nên mã cụm 0, 1, 2, 3 không cố định ý nghĩa (hiện tượng Label Switching). Làm sao hệ thống tự động gán đúng nhãn tiếng Việt mà không phải gán cứng thủ công?",
                "short": "Hệ thống giải quyết bằng quy trình Chuẩn hóa thứ tự tâm cụm hậu nghiệm (Post-hoc Centroid Canonicalization): sắp xếp lại mã cụm 0 → K-1 theo thứ tự tăng dần của tỷ lệ ca mắc trung bình, sau đó biến đổi ngược tâm cụm về không gian gốc (scaler.inverse_transform) để đọc độ lớn và chiều tăng/giảm tuần rồi tự ghép thành nhãn tiếng Việt.",
                "points": [
                    ("Không bao giờ bị đảo màu hay tráo nhãn trên Dashboard: ", "Dù chạy lại trên dữ liệu năm mới, Cụm 0 luôn cố định là 'Thấp – ổn định' (màu xanh) và Cụm 3 luôn là 'Rất cao – ổn định' (màu đỏ). Hướng biến động tâm cụm xét theo ngưỡng log-change (±0,15 tương đương ±16% thay đổi ca/tuần), nhờ đó tự động phát hiện chính xác Cụm 2 là 'Cao – đang tăng' (+705%/tuần)."),
                ],
            },
            {
                "q": "Câu 15: Trong bài toán phân cụm, nhóm áp dụng giảm chiều PCA trước hay sau khi chạy K-Means? Tại sao?",
                "short": "Nhóm chạy K-Means trực tiếp trên đủ 3 đặc trưng chuẩn hóa trước, rồi mới dùng PCA chiếu xuống 2 thành phần chính (pca_1, pca_2) để trực quan hóa. Vì không gian đầu vào chỉ có 3 chiều đều mang ý nghĩa dịch tễ cốt lõi (ca mắc, tử vong, đà tăng), nếu nén PCA trước xuống 2 chiều sẽ làm rơi mất một phần phương sai thông tin không cần thiết.",
                "points": [
                    ("Đúng vai trò của PCA trong hệ thống: ", "Hai trục pca_1 (đại diện độ lớn ca mắc & tử vong) và pca_2 (đại diện biến động tăng trưởng tuần) giúp hiển thị toàn bộ các cụm trên biểu đồ mặt phẳng 2D trực quan tại Dashboard 2 Superset mà vẫn bảo toàn độ chính xác phân cụm 3D gốc."),
                ],
            },
            {
                "q": "Câu 16: Phân tích Chuỗi chuyển dịch cụm Markov và Thời gian lưu trú (Dwell Time) mang lại phát hiện mới nào mà riêng K-Means không thể cho biết?",
                "short": "K-Means chỉ mô tả trạng thái tĩnh ở từng tuần rời rạc, còn Ma trận Markov bậc 1 và Dwell Time lượng hóa quy luật vận động theo thời gian: phát hiện Cụm 2 ('Cao – đang tăng') chỉ là trạng thái quá độ cực ngắn (lưu trú trung bình 1,02 tuần/đợt, xác suất giữ cụm 1,95%), trong khi một khi rơi vào Cụm 3 ('Rất cao') thì đợt đỉnh dịch kéo dài dai dẳng trung bình 5,65 tuần liên tiếp (giữ cụm 82,30%).",
                "points": [
                    ("Giá trị hỗ trợ ra quyết định y tế: ", "Từ Cụm 1 (Trung bình), xác suất leo thang thẳng lên Vùng Đỏ (Cụm 3) là 6,14%/tuần (914 lần trong lịch sử); và khi đã ở Vùng Đỏ, hệ thống bệnh viện phải chuẩn bị oxy/giường hồi sức cho chu kỳ trung bình ~6 tuần thay vì kỳ vọng dịch tự hạ nhiệt sau 1–2 tuần. Ngoài ra trên Cổng AI (:8501), phép nhân lũy thừa ma trận P^k cho phép dự phóng phân phối xác suất trạng thái sau k = 1..16 tuần."),
                ],
            },
        ],
    },
    {
        "section_title": "PHẦN 4: CẢNH BÁO SỚM XGBOOST, CHỐNG RÒ RỈ THỜI GIAN & GIẢI THÍCH TREESHAP (CÂU 17 – CÂU 21)",
        "items": [
            {
                "q": "Câu 17: Tại sao nhãn mục tiêu (Target Label) của bài toán Cảnh báo sớm lại yêu cầu đồng thời 2 điều kiện: tỷ lệ ca mắc sau 2 tuần tăng gấp ≥ 1,5 lần VÀ đạt tối thiểu ≥ 10 ca / 100.000 dân?",
                "short": "Kết hợp ngưỡng tương đối (≥ 1,5x) và ngưỡng sàn tuyệt đối (≥ 10 ca/100k dân) nhằm loại bỏ triệt để 'nhiễu mẫu nhỏ' (Small-number Noise). Nếu chỉ xét tăng 50%, một nước tăng từ 1 ca lên 2 ca/100k dân (+100%) sẽ bị gán nhãn bùng phát giả, làm mô hình học sai quy luật dịch tễ thực chất.",
                "points": [
                    ("Ý nghĩa của khoảng dự báo trước H = 2 tuần (Horizon): ", "Chu kỳ ủ bệnh và lây truyền thứ cấp của SARS-CoV-2 kéo dài trung bình 5–14 ngày; cảnh báo sớm trước 2 tuần là khoảng thời gian 'vàng' vừa đủ độ tin cậy tín hiệu động lượng vừa kịp để địa phương kích hoạt trạm y tế lưu động và tiêm chủng tăng cường."),
                ],
            },
            {
                "q": "Câu 18: Cơ chế chia tập Purged Time-Series Split (chia theo thời gian kèm khoảng trống Purge 2 tuần) hoạt động thế nào và tại sao không dùng K-Fold hay train_test_split ngẫu nhiên?",
                "short": "Dữ liệu dịch bệnh có tính tự tương quan chuỗi thời gian và xu hướng toàn cầu theo đợt biến chủng. Chia ngẫu nhiên (shuffle=True) sẽ gây rò rỉ tương lai (Data Leakage) nghiêm trọng. Ngay cả khi cắt theo mốc thời gian cutoff, các mẫu cuối tập Train (từ cutoff - H đến cutoff - 1) có nhãn mục tiêu y_t được tính từ số ca ở tuần t + H nằm bên trong vùng thời gian của tập Test, nên bắt buộc phải cắt bỏ (purge) khoảng giao thoa H = 2 tuần đó khỏi tập Train.",
                "points": [
                    ("Kiểm chứng tính nhân quả nghiêm ngặt bằng Unit Test: ", "Trong airflow/tests/test_covid_early_warning.py, nhóm viết bài test thay đổi đột biến số ca ở tuần t+1 lên gấp 1.000 lần và chứng minh toàn bộ 18 đặc trưng đầu vào tại tuần t không thay đổi dù chỉ 1 chữ số thập phân — đảm bảo 100% không rò rỉ đặc trưng tương lai."),
                ],
            },
            {
                "q": "Câu 19: Đặc trưng cluster_id đưa vào XGBoost được sinh ra từ mô hình K-Means chạy trên toàn bộ 56.477 dòng. Liệu điều này có làm rò rỉ thông tin từ tập Test sang tập Train và làm phồng điểm ROC-AUC = 0,925 không?",
                "short": "Nhóm đã kiểm chứng thực nghiệm trực tiếp bằng nghiên cứu loại bỏ biến (Ablation Study: đặt COVID_EW_INCLUDE_CLUSTER=false để bỏ hoàn toàn cluster_id). Kết quả trên cùng 11.006 mẫu kiểm thử độc lập đạt ROC-AUC = 0,926, F1 = 0,355 và PR-AUC = 0,264 (so với 0,925 / 0,356 / 0,270 khi có cluster_id), chứng minh mô hình hoàn toàn trung thực, không hề bị phồng điểm ảo.",
                "points": [
                    ("Giải thích về mặt toán học: ", "K-Means chỉ gom cụm không giám sát dựa trên mức ca mắc, tử vong và biến động tại tuần hiện tại t (hoàn toàn không nhìn nhãn bùng phát ở t+2), đồng thời tâm cụm đã hội tụ rất ổn định trên 44.520 mẫu lịch sử đầu tiên. Việc có cluster_id giúp cải thiện nhẹ độ nhạy lớp hiếm PR-AUC thêm +0,006 (từ 0,264 lên 0,270)."),
                ],
            },
            {
                "q": "Câu 20: Tại sao trên tập kiểm thử (11.006 dòng), XGBoost đạt ROC-AUC rất cao (0,925) nhưng PR-AUC chỉ đạt 0,270 và Precision đạt 0,244? Kết quả này tốt hay kém?",
                "short": "Đây là kết quả rất tốt và phản ánh trung thực đặc thù Mất cân bằng lớp cực mạnh (Extreme Class Imbalance) ở giai đoạn hậu đại dịch 2023–2024 khi số tuần bùng phát thực sự rất hiếm (~3–4%). Trên dữ liệu hiếm, đường cơ sở luật chỉ đạt PR-AUC = 0,070; việc XGBoost đạt PR-AUC = 0,270 cao gấp gần 4 lần luật cơ sở và cao hơn +63,6% so với Hồi quy Logistic (0,165), đồng thời bắt trúng 66,3% các đợt bùng phát thực tế trước 2 tuần (Recall = 0,663).",
                "points": [
                    ("Đánh đổi Precision – Recall trong y tế dự phòng: ", "Chi phí của việc bỏ sót một đợt bùng phát dịch (False Negative) lớn hơn rất nhiều so với chi phí phát tín hiệu cảnh báo sớm để theo dõi dịch tễ (False Positive). Ngưỡng quyết định 0,64 được tối ưu tự động trên tập validation cùng trọng số lớp hiếm scale_pos_weight giúp cân bằng tốt F1 = 0,356 (vượt xa Logistic 0,282 và Luật cơ sở 0,161)."),
                ],
            },
            {
                "q": "Câu 21: Sự khác biệt giữa biểu đồ Top 10 Feature Importance toàn cục và giải thích TreeSHAP cục bộ (top_reasons_vi) trong hệ thống của nhóm là gì?",
                "short": "Feature Importance (Gain %) chỉ cho biết tính trung bình trên toàn bộ tập huấn luyện biến nào hay được dùng để rẽ nhánh nhất, còn TreeSHAP (tính chính xác trên cây quyết định qua pred_contribs=True) giải thích riêng cho TỪNG quốc gia ở TỪNG tuần cụ thể rằng 3 yếu tố nào đang đẩy xác suất bùng phát tăng lên (+) hoặc kéo giảm xuống (-).",
                "points": [
                    ("Ví dụ thực tế từ DWH: ", "Cùng sử dụng một mô hình XGBoost, ở Romania xác suất bùng phát vọt cao được TreeSHAP chỉ rõ do 'Tốc độ tăng 2 tuần (+0,58); Tốc độ tăng 1 tuần (+0,54)', trong khi ở quốc gia an toàn cột top_reasons_vi giải thích rõ 'Mức ca mắc hiện tại (giảm rủi ro -0,90); TB ca mắc 4 tuần (giảm rủi ro -0,62)' — xóa bỏ tính hộp đen (black-box) của mô hình."),
                ],
            },
        ],
    },
    {
        "section_title": "PHẦN 5: KHAI PHÁ LUẬT KẾT HỢP APRIORI & PHÁT HIỆN BẤT THƯỜNG KÉP (CÂU 22 – CÂU 25)",
        "items": [
            {
                "q": "Câu 22: Trong bài toán Khai phá luật kết hợp trên hồ sơ bệnh nhân CDC, tại sao nhóm xếp hạng luật ưu tiên theo chỉ số Lift (Độ nâng) thay vì chỉ dùng Support và Confidence?",
                "short": "Nếu chỉ dùng Confidence P(B|A), khi kết cục B vốn chiếm tỷ lệ lớn trong dữ liệu thì mọi tập điều kiện A ngẫu nhiên đều có Confidence cao giả tạo. Chỉ số Lift = P(B|A) / P(B) đo mức tăng xác suất thực chất so với nền chung: Lift > 1 khẳng định mối tương quan dương có ý nghĩa thống kê.",
                "points": [
                    ("Minh chứng từ luật số 1 trong hệ thống: ", "Luật [Bệnh nền: Tăng huyết áp + Tiêm chủng: Mới tiêm 1 mũi] ⇒ [Kết quả: Dương tính COVID-19] đạt Confidence = 73,81% và Lift = 2,70x (kèm Conviction cao), nghĩa là nhóm bệnh nhân có bệnh nền tăng huyết áp mà chưa tiêm đủ liều cơ bản có nguy cơ dương tính cao gấp 2,7 lần tỷ lệ chung — gợi ý hành động rất cụ thể là ưu tiên nhắc lịch tiêm mũi 2 & mũi nhắc lại cho nhóm này."),
                ],
            },
            {
                "q": "Câu 23: Bảng dim_patient và fact_vaccination là dữ liệu quan hệ có cả số nguyên (năm sinh, số mũi tiêm). Nhóm đã biến đổi sang dạng giỏ hàng giao dịch (Transactional Itemsets) cho thuật toán Apriori như thế nào?",
                "short": "Mỗi bệnh nhân ẩn danh (loại trừ dòng placeholder) được tổng hợp số mũi tiêm và loại vắc-xin chính từ fact_vaccination, sau đó rời rạc hóa ngữ nghĩa (Semantic Discretization) thành 5 nhóm hạng mục vế trái (Nhóm tuổi lúc khai báo, Giới tính, Bệnh nền, Cấp độ mũi tiêm 0 / 1 / 2 / ≥3 mũi, Loại vắc-xin) và cố định hạng mục kết cục sức khỏe làm vế phải (Consequent Constraint).",
                "points": [
                    ("Tối ưu không gian tìm kiếm: ", "Việc khóa vế phải chỉ nhận giá trị Kết quả sức khỏe (Dương tính, Nghi nhiễm, Đã khỏi, Bình thường) với ngưỡng min_support = 1,5%, min_confidence = 20%, min_lift = 1,05 và độ dài vế trái tối đa 3 hạng mục giúp loại bỏ hàng nghìn luật dư thừa không mang giá trị y khoa (như suy luận giới tính từ loại vắc-xin)."),
                ],
            },
            {
                "q": "Câu 24: Tại sao nhóm dùng thuật toán Isolation Forest để phát hiện 1.130 tuần bất thường lịch sử thay vì chỉ dùng ngưỡng thống kê Z-Score > 3 đơn giản?",
                "short": "Z-Score đơn biến giả định dữ liệu phân phối chuẩn và chỉ nhìn từng cột riêng lẻ, trong khi Isolation Forest hoạt động phi tham số trên không gian đa chiều (5 đặc trưng: log ca mắc, log tử vong, Z-score ca mắc nội quốc gia, Z-score tử vong và CFR tuần), giúp bắt được cả các bất thường tổ hợp phi tuyến và nhiễu báo cáo.",
                "points": [
                    ("Phân loại hậu nghiệm 3 nhóm bất thường rõ ràng: ", "Sau khi Isolation Forest cô lập 2% điểm dị thường nhất (1.130 / 56.477 tuần – quốc gia), tầng diễn giải phân loại thành: (1) Nhiễu dồn / hiệu chỉnh báo cáo cộng dồn (570 tuần — rất phổ biến khi các nước xả số liệu dồn sau kỳ nghỉ), (2) Tử vong tăng dị thường lệch pha ca mắc (532 tuần), và (3) Đột biến ca mắc cục bộ thực sự (28 tuần)."),
                ],
            },
            {
                "q": "Câu 25: Bộ phát hiện ổ dịch thời gian thực trên Kafka (StreamingOutbreakDetector) làm cách nào để phát hiện bùng phát tại một tỉnh trong < 3 giây nhưng không bị báo động giả lúc nạp dữ liệu ban đầu?",
                "short": "Bộ dò trực tuyến trong cdc/consumer.py sử dụng cấu trúc cửa sổ trượt bộ nhớ trong (In-memory Sliding Window bằng collections.deque 300 giây cho từng tỉnh), chỉ kích hoạt với sự kiện tạo mới/cập nhật trực tuyến (op thuộc 'c', 'u') và bỏ qua hoàn toàn sự kiện snapshot lịch sử (op = 'r'), đồng thời áp cơ chế thời gian nghỉ (cooldown 30s) chống lặp cảnh báo.",
                "points": [
                    ("Công thức Z-score so sánh chéo liên tỉnh thời gian thực: ", "Khi số ca CONFIRMED_COVID tại một tỉnh trong 300 giây vượt ngưỡng burst_threshold (8 ca), hệ thống đối chiếu ngay với trung bình số ca cùng cửa sổ của các tỉnh còn lại để tính Z-score động (ví dụ kịch bản demo Hà Nội đạt Z-score = 4,95, mức CẢNH BÁO CAO) và ghi tức thì vào dwh.fact_realtime_anomalies để hiển thị cả trên Superset Dashboard 4 lẫn biểu ngữ đỏ của Cổng AI (:8501)."),
                ],
            },
        ],
    },
    {
        "section_title": "PHẦN 6: TRỰC QUAN HÓA BI, CỔNG AI TƯƠNG TÁC, KIỂM THỬ & MỞ RỘNG (CÂU 26 – CÂU 28)",
        "items": [
            {
                "q": "Câu 26: Làm thế nào nhóm khởi tạo tự động và Việt hóa 100% cả 17 Datasets, 28 Biểu đồ và 4 Dashboard trên Apache Superset mà không cần thao tác kéo thả thủ công trên giao diện?",
                "short": "Nhóm xây dựng kịch bản BI-as-Code (scripts/init_superset_dashboards.py) giao tiếp trực tiếp với Superset REST API v1 (/api/v1/security/login, /dataset, /chart, /dashboard) chỉ bằng thư viện chuẩn Python, thực hiện Việt hóa xuyên suốt 3 lớp từ SQL View (*_vi), từ điển nhãn cột (verbose_name) đến cấu trúc lưới bố cục 12 cột.",
                "points": [
                    ("Lợi ích vượt trội khi triển khai & chấm thi: ", "Thông thường Superset lưu biểu đồ trong metadata DB riêng nên khi sang máy khác phải dựng lại từ đầu hoặc import file zip dễ lỗi UUID. Với script REST API lũy đẳng của nhóm, chỉ cần 1 lệnh chạy trong ~10 giây là toàn bộ 17 Dataset và 28 biểu đồ được cập nhật tại chỗ (in-place), không tạo bản trùng, không sót bất kỳ tên cột tiếng Anh thô nào."),
                ],
            },
            {
                "q": "Câu 27: Hệ thống đã có 4 Dashboard BI rất đầy đủ trên Apache Superset (cổng 8089), vậy tại sao nhóm còn xây dựng thêm Cổng Web AI Tương tác bằng Streamlit (cổng 8501)?",
                "short": "Superset phục vụ phân tích mô tả & chẩn đoán (Descriptive BI) trên dữ liệu đã tính sẵn trong DWH, nhưng không thể nhận tham số giả định mới từ người dùng để gọi file mô hình học máy chạy suy luận thời gian thực. Streamlit AI Portal bổ sung tầng Phân tích hỗ trợ quyết định tương tác (Interactive What-If & Prescriptive Analytics).",
                "points": [
                    ("4 tính năng tương tác trực tiếp trên mô hình thật tại cổng 8501: ", "(1) Kéo thanh trượt giả định độ phủ vắc-xin / tốc độ tăng ca để chạy predict_proba và vẽ biểu đồ phân rã TreeSHAP trực tiếp từ early_warning_model.pkl; (2) Nhập hồ sơ bệnh nhân để đối chiếu tức thì với tập luật Apriori; (3) Khảo sát quỹ đạo từng quốc gia kèm dấu X bất thường Isolation Forest và tính lũy thừa ma trận Markov P^k cho k = 1..16 tuần tới; (4) Giám sát bảng cảnh báo ổ dịch Kafka theo giờ Việt Nam."),
                ],
            },
            {
                "q": "Câu 28: Nếu mở rộng hệ thống từ quy mô đồ án (vài trăm nghìn bản ghi trên 1 máy Docker Compose) lên quy mô quốc gia (hàng trăm triệu bản ghi bệnh nhân/mũi tiêm mỗi năm), kiến trúc hiện tại có điểm nghẽn nào và hướng nâng cấp ra sao?",
                "short": "Nhờ kiến trúc đã tách biệt rõ 5 tầng độc lập (Source – Ingestion/CDC – Data Lake – DWH – Mining/Serving), luồng thiết kế không phải đập đi xây lại mà chỉ cần nâng cấp theo chiều ngang tại 3 điểm nghẽn chính: bộ tiêu thụ streaming, phân vùng kho dữ liệu và huấn luyện phân tán/trực tuyến.",
                "points": [
                    ("Tầng Streaming CDC: ", "Tăng số partition của Kafka topics theo khóa băm patient_id và thay cdc-consumer đơn tiến trình bằng cụm worker song song (Consumer Group nhiều replica) hoặc Apache Flink / Spark Structured Streaming."),
                    ("Tầng Kho dữ liệu: ", "Áp dụng Declarative Range Partitioning theo năm/tháng cho bảng lớn trên PostgreSQL kết hợp nạp增量 (Incremental Merge chỉ nạp các ngày mới thay vì nạp lại toàn bộ lịch sử OWID), hoặc chuyển tầng OLAP sang ClickHouse / Google BigQuery."),
                    ("Tầng Học máy: ", "Nâng cấp K-Means mẻ tuần sang Online Mini-Batch K-Means để cập nhật tâm cụm liên tục theo dòng sự kiện, kết hợp giám sát trôi dạt phân phối dữ liệu (Concept Drift) và dự báo định lượng khoảng tin cậy số ca (Quantile Forecasting)."),
                ],
            },
        ],
    },
]


# ==============================================================================
# HÀM TIỆN ÍCH ĐỊNH DẠNG WORD (.DOCX) CHUẨN HỌC THUẬT PTIT
# ==============================================================================
def set_run_font(run, size_pt=12.0, bold=False, italic=False, color=DARK, font_name="Times New Roman"):
    run.font.name = font_name
    run.font.size = Pt(size_pt)
    run.bold = bold
    run.italic = italic
    if color is not None:
        run.font.color.rgb = color
    rPr = run._element.get_or_add_rPr()
    rFonts = rPr.find(qn("w:rFonts"))
    if rFonts is None:
        rFonts = OxmlElement("w:rFonts")
        rPr.insert(0, rFonts)
    for attr in ("w:ascii", "w:hAnsi", "w:cs", "w:eastAsia"):
        rFonts.set(qn(attr), font_name)


def add_page_number(run):
    fldChar1 = parse_xml(r'<w:fldChar %s w:fldCharType="begin"/>' % nsdecls("w"))
    instrText = parse_xml(r'<w:instrText %s xml:space="preserve"> PAGE </w:instrText>' % nsdecls("w"))
    fldChar2 = parse_xml(r'<w:fldChar %s w:fldCharType="separate"/>' % nsdecls("w"))
    fldChar3 = parse_xml(r'<w:fldChar %s w:fldCharType="end"/>' % nsdecls("w"))
    run._r.extend([fldChar1, instrText, fldChar2, fldChar3])


def shade_cell(cell, hex_color: str):
    cell._tc.get_or_add_tcPr().append(parse_xml(f'<w:shd {nsdecls("w")} w:fill="{hex_color}"/>'))


def set_cell_margins(cell, top=95, bottom=95, left=130, right=130):
    tcPr = cell._tc.get_or_add_tcPr()
    tcMar = OxmlElement("w:tcMar")
    for m, val in (("top", top), ("bottom", bottom), ("left", left), ("right", right)):
        node = OxmlElement(f"w:{m}")
        node.set(qn("w:w"), str(val))
        node.set(qn("w:type"), "dxa")
        tcMar.append(node)
    tcPr.append(tcMar)


def add_styled_table(doc: Document, headers: list[str], rows: list[list[str]], widths_cm: list[float]):
    tbl = doc.add_table(rows=len(rows) + 1, cols=len(headers))
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl._tbl.tblPr.append(
        parse_xml(
            f"""
            <w:tblBorders {nsdecls("w")}>
                <w:top w:val="single" w:sz="6" w:space="0" w:color="1E3A8A"/>
                <w:bottom w:val="single" w:sz="8" w:space="0" w:color="1E3A8A"/>
                <w:left w:val="none"/><w:right w:val="none"/>
                <w:insideH w:val="single" w:sz="4" w:space="0" w:color="CBD5E1"/>
                <w:insideV w:val="single" w:sz="4" w:space="0" w:color="E2E8F0"/>
            </w:tblBorders>
            """
        )
    )
    tbl.rows[0]._tr.get_or_add_trPr().append(parse_xml(f'<w:tblHeader {nsdecls("w")}/>'))
    for c_idx, h in enumerate(headers):
        cell = tbl.rows[0].cells[c_idx]
        shade_cell(cell, "1E3A8A")
        set_cell_margins(cell, top=105, bottom=105, left=110, right=110)
        cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
        cell.width = Cm(widths_cm[c_idx])
        p = cell.paragraphs[0]
        p.paragraph_format.space_after = Pt(0)
        r = p.add_run(h)
        set_run_font(r, size_pt=10.5, bold=True, color=WHITE)

    for r_idx, row_vals in enumerate(rows):
        bg = "F8FAFC" if r_idx % 2 == 1 else "FFFFFF"
        for c_idx, val in enumerate(row_vals):
            cell = tbl.rows[r_idx + 1].cells[c_idx]
            shade_cell(cell, bg)
            set_cell_margins(cell, top=80, bottom=80, left=110, right=110)
            cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
            cell.width = Cm(widths_cm[c_idx])
            p = cell.paragraphs[0]
            p.paragraph_format.line_spacing = 1.18
            p.paragraph_format.space_after = Pt(0)
            r = p.add_run(str(val))
            set_run_font(r, size_pt=10.5, bold=(c_idx == 0), color=DARK)

    sp = doc.add_paragraph()
    sp.paragraph_format.space_after = Pt(4)


def add_section_banner(doc: Document, title: str):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(14)
    p.paragraph_format.space_after = Pt(6)
    p.paragraph_format.keep_with_next = True
    p._p.get_or_add_pPr().append(
        parse_xml(
            f'<w:pBdr {nsdecls("w")}><w:bottom w:val="single" w:sz="12" w:space="4" w:color="1E3A8A"/></w:pBdr>'
        )
    )
    r = p.add_run(title)
    set_run_font(r, size_pt=13.5, bold=True, color=NAVY)


def add_qa_block(doc: Document, q_text: str, short_ans: str, points: list[tuple[str, str]]):
    # Khung câu hỏi (viền trái xanh đậm + nền xanh nhạt #EFF6FF)
    q_tbl = doc.add_table(rows=1, cols=1)
    q_tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    q_cell = q_tbl.cell(0, 0)
    q_cell.width = Cm(16.5)
    shade_cell(q_cell, "EFF6FF")
    set_cell_margins(q_cell, top=100, bottom=100, left=150, right=140)
    q_cell._tc.get_or_add_tcPr().append(
        parse_xml(
            f"""
            <w:tcBorders {nsdecls("w")}>
                <w:top w:val="none"/><w:right w:val="none"/><w:bottom w:val="none"/>
                <w:left w:val="single" w:sz="24" w:space="0" w:color="1E3A8A"/>
            </w:tcBorders>
            """
        )
    )
    qp = q_cell.paragraphs[0]
    qp.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    qp.paragraph_format.line_spacing = 1.25
    qp.paragraph_format.space_after = Pt(0)
    qp.paragraph_format.keep_with_next = True
    qr = qp.add_run(q_text)
    set_run_font(qr, size_pt=12.0, bold=True, color=NAVY)

    # Đoạn trả lời nhanh 30 giây
    sp_p = doc.add_paragraph()
    sp_p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    sp_p.paragraph_format.space_before = Pt(5)
    sp_p.paragraph_format.space_after = Pt(3.5)
    sp_p.paragraph_format.line_spacing = 1.28
    r_tag = sp_p.add_run("► Trả lời cốt lõi (30 giây): ")
    set_run_font(r_tag, size_pt=11.5, bold=True, color=TEAL)
    r_short = sp_p.add_run(short_ans)
    set_run_font(r_short, size_pt=11.5, bold=True, color=DARK)

    # Các ý giải thích sâu & dẫn chứng code/số liệu
    for lead, body in points:
        bp = doc.add_paragraph()
        bp.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        bp.paragraph_format.left_indent = Cm(0.65)
        bp.paragraph_format.first_line_indent = Cm(-0.35)
        bp.paragraph_format.line_spacing = 1.25
        bp.paragraph_format.space_after = Pt(2.5)
        r_dot = bp.add_run("•  ")
        set_run_font(r_dot, size_pt=11.5, bold=True, color=NAVY)
        r_lead = bp.add_run(lead)
        set_run_font(r_lead, size_pt=11.5, bold=True, color=DARK)
        r_body = bp.add_run(body)
        set_run_font(r_body, size_pt=11.5, color=SLATE)

    gap = doc.add_paragraph()
    gap.paragraph_format.space_after = Pt(6)


def build_qa_document(output_path: Path) -> None:
    doc = Document()
    for sec in doc.sections:
        sec.page_width = Cm(21.0)
        sec.page_height = Cm(29.7)
        sec.top_margin = Cm(2.0)
        sec.bottom_margin = Cm(2.0)
        sec.left_margin = Cm(2.5)
        sec.right_margin = Cm(2.0)
        sec.different_first_page_header_footer = True

        hdr = sec.header.paragraphs[0]
        hdr.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        rh = hdr.add_run("Bộ 28 Câu hỏi & Đáp án Phản biện Bảo vệ Đồ án — Kho dữ liệu & Khai phá dữ liệu")
        set_run_font(rh, size_pt=9.5, italic=True, color=MUTED)

        ftr = sec.footer.paragraphs[0]
        ftr.alignment = WD_ALIGN_PARAGRAPH.CENTER
        rf1 = ftr.add_run("Trang ")
        set_run_font(rf1, size_pt=10.0, color=MUTED)
        rpg = ftr.add_run()
        set_run_font(rpg, size_pt=10.0, bold=True, color=NAVY)
        add_page_number(rpg)

    # Tiêu đề trang đầu
    p_org = doc.add_paragraph()
    p_org.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_org.paragraph_format.space_after = Pt(2)
    r = p_org.add_run("HỌC VIỆN CÔNG NGHỆ BƯU CHÍNH VIỄN THÔNG (PTIT)\nHỌC PHẦN: KHO DỮ LIỆU VÀ KHAI PHÁ DỮ LIỆU (INT1422)")
    set_run_font(r, size_pt=11.5, bold=True, color=TEAL)

    p_title = doc.add_paragraph()
    p_title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_title.paragraph_format.space_before = Pt(6)
    p_title.paragraph_format.space_after = Pt(4)
    r = p_title.add_run(
        "BỘ 28 CÂU HỎI & ĐÁP ÁN PHẢN BIỆN BẢO VỆ ĐỒ ÁN\n"
        "NỀN TẢNG DỮ LIỆU ĐA NGUỒN & KHAI PHÁ DỮ LIỆU COVID-19"
    )
    set_run_font(r, size_pt=16.5, bold=True, color=NAVY)

    p_sub = doc.add_paragraph()
    p_sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_sub.paragraph_format.space_after = Pt(10)
    r = p_sub.add_run(
        "Cấu trúc mỗi câu gồm: (1) Trả lời cốt lõi ngắn gọn 30 giây trên bàn bảo vệ & "
        "(2) Phân tích chuyên sâu kèm dẫn chứng mã nguồn, công thức và số liệu thực nghiệm."
    )
    set_run_font(r, size_pt=11.0, italic=True, color=SLATE)

    # Bảng nhẩm nhanh thông số "vàng" trước khi bước lên bảo vệ
    p_tbl_hdr = doc.add_paragraph()
    p_tbl_hdr.paragraph_format.space_after = Pt(4)
    r = p_tbl_hdr.add_run("BẢNG TÓM TẮT THÔNG SỐ CỐT LÕI CẦN NHỚ KHI HỘI ĐỒNG HỎI NHANH:")
    set_run_font(r, size_pt=11.5, bold=True, color=NAVY)

    add_styled_table(
        doc,
        headers=["Hạng mục hệ thống", "Con số & Chỉ số thực nghiệm chính xác", "Tệp mã nguồn / Bảng DWH đối chứng"],
        rows=[
            ["Quy mô hạ tầng & dữ liệu", "11 containers | ~395.000 dòng OWID (ISO-3) | 56.477 tuần–QG | 1.508+ BN & 3.332+ mũi tiêm CDC", "docker-compose.yml | dwh.fact_cases | dwh.dim_patient"],
            ["Phân cụm K-Means & PCA", "Kiểm định K = 2..8 | K=4 được chọn (Sil = 0,5478) | K=5 đạt đỉnh Sil = 0,5821 | 4 nhãn tự sinh + PCA 2D", "weekly_covid_kmeans.py | dwh.fact_kmeans_evaluation"],
            ["Chuỗi chuyển cụm Markov", "Lưu trú TB: Cụm 0 (17,15w - giữ 94,8%), Cụm 1 (6,38w), Cụm 2 quá độ (1,02w - 1,95%), Cụm 3 đỉnh dịch (5,65w - 82,3%)", "covid_cluster_transitions.py | dwh.fact_cluster_dwell_time"],
            ["Cảnh báo sớm XGBoost + SHAP", "Dự báo trước H=2 tuần (Purge gap 2w, 44.520 Train / 11.006 Test) | ROC-AUC = 0,925 | PR-AUC = 0,270 | F1 = 0,356 | Ablation = 0,926 / 0,264", "covid_early_warning.py | dwh.fact_early_warning (+top_reasons_vi)"],
            ["Luật kết hợp Apriori (CDC)", "Top 40 luật | Luật #1: [Tăng huyết áp + Mới tiêm 1 mũi] ⇒ Dương tính (Conf = 73,81%, Lift = 2,70x)", "patient_association_rules.py | dwh.fact_patient_association_rules"],
            ["Phát hiện bất thường kép", "Isolation Forest: 1.130 tuần dị thường (570 nhiễu báo cáo, 532 tử vong, 28 đột biến ca) | Kafka CDC: < 3s (Z-score = 4,95)", "covid_anomaly_detection.py | cdc/consumer.py"],
            ["Trực quan hóa & Kiểm thử", "Superset (:8089): 17 Datasets, 28 biểu đồ, 4 Dashboard | Streamlit AI Portal (:8501): 4 Tab | 28/28 Unit Tests PASS", "init_superset_dashboards.py | portal/app.py"],
        ],
        widths_cm=[4.2, 7.6, 4.7],
    )

    # Mục lục nhanh 6 phần
    add_styled_table(
        doc,
        headers=["Nhóm chủ đề phản biện", "Phạm vi câu hỏi", "Trọng tâm giảng viên thường xoáy sâu"],
        rows=[
            ["Phần 1: Kiến trúc, ETL & Snowflake DWH", "Câu 1 – Câu 5", "Xung đột ETL vs CDC, lý do chọn Snowflake & MinIO, tách bảng OWID/WHO, tính Idempotent."],
            ["Phần 2: Log-based CDC, Kafka & Ẩn danh PII", "Câu 6 – Câu 10", "Debezium Binlog vs Polling, xử lý lệch thứ tự Kafka (Placeholder), ẩn danh PII, chống mất/trùng offset."],
            ["Phần 3: K-Means, PCA & Chuỗi Markov", "Câu 11 – Câu 16", "Chuẩn hóa log1p/Z-score, chọn K=4 vs K=5, tối ưu O(N²) Silhouette, chống tráo nhãn, ý nghĩa Dwell Time."],
            ["Phần 4: XGBoost, Purged Split & TreeSHAP", "Câu 17 – Câu 21", "Định nghĩa nhãn bùng phát, chống Data Leakage (Purge 2w & Ablation), giải thích PR-AUC vs ROC-AUC, TreeSHAP."],
            ["Phần 5: Luật Apriori & Bất thường Kép", "Câu 22 – Câu 25", "Vai trò độ đo Lift, rời rạc hóa giỏ bệnh nhân, Isolation Forest đa chiều vs Z-score, chống spam cảnh báo Kafka."],
            ["Phần 6: Superset, AI Portal & Mở rộng", "Câu 26 – Câu 28", "Tự động hóa REST API Việt hóa 28 biểu đồ, khác biệt Superset vs Streamlit What-If, hướng mở rộng Big Data."],
        ],
        widths_cm=[5.2, 2.8, 8.5],
    )

    # Nội dung chi tiết 28 câu hỏi
    total_q = 0
    for sec in QA_SECTIONS:
        add_section_banner(doc, sec["section_title"])
        for item in sec["items"]:
            total_q += 1
            add_qa_block(doc, item["q"], item["short"], item["points"])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(output_path))
    print(f"[+] Đã xuất thành công {total_q} câu hỏi phản biện & đáp án tại: {output_path} ({output_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    build_qa_document(DEFAULT_OUTPUT)
