# -*- coding: utf-8 -*-
"""
Cập nhật đồng bộ tại chỗ (in-place, giữ nguyên trang bìa PTIT, danh sách nhóm,
theme & slide master gốc) cho cả hai file trong thư mục `baocao/`:
  1. baocao/baocao-datamining.docx
  2. baocao/slide-datamining.pptx

Bổ sung đầy đủ các nâng cấp mới nhất của hệ thống:
  - CDC v2 ẩn danh hóa bệnh nhân (dwh.dim_patient + Placeholder + 1.508 bệnh nhân / 3.332 mũi tiêm)
  - K-Means nâng cao (Kiểm định K = 2..8, Hồ sơ cụm tự sinh nhãn tiếng Việt, PCA 2 chiều)
  - Khai phá chuỗi chuyển dịch cụm Markov & Thời gian lưu trú (Dwell Time)
  - Hệ thống Cảnh báo sớm bùng phát dịch bằng XGBoost (Purged Time Split + Ablation)
  - Khai phá Luật kết hợp Apriori / FP-Growth trên hồ sơ bệnh nhân & tiêm chủng
  - Tự động hóa & Việt hóa 4 Dashboard (15 Datasets, 25 biểu đồ) qua Superset REST API
  - Bộ kiểm thử đơn vị tự động 26/26 Unit Tests
"""

import tempfile
import zipfile
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL as WD_CELL_VERTICAL
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import nsdecls, qn
from docx.shared import Cm, Inches, Pt, RGBColor
from pptx import Presentation
from pptx.dml.color import RGBColor as PptRGB
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.util import Inches as PptIn, Pt as PptPt

plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Arial", "Liberation Sans"]
plt.rcParams["axes.unicode_minus"] = False

REPO_ROOT = Path(__file__).resolve().parents[1]
BAOCAO_DOCX = REPO_ROOT / "baocao" / "baocao-datamining.docx"
SLIDE_PPTX = REPO_ROOT / "baocao" / "slide-datamining.pptx"
GIOI_THIEU_DOCX = REPO_ROOT / "GIOI_THIEU_HE_THONG.docx"

NAVY = RGBColor(0x1E, 0x3A, 0x8A)
TEAL = RGBColor(0x0F, 0x76, 0x6E)
SLATE = RGBColor(0x33, 0x41, 0x55)
DARK = RGBColor(0x0F, 0x17, 0x2A)


# ==============================================================================
# 1. CHUẨN BỊ HÌNH ẢNH 240 DPI CHO CẢ BÁO CÁO WORD VÀ SLIDE POWERPOINT
# ==============================================================================
def prepare_figures(tmp_dir: Path) -> dict[str, Path]:
    figs = {}
    if GIOI_THIEU_DOCX.exists():
        with zipfile.ZipFile(GIOI_THIEU_DOCX) as z:
            for key, media_name in (
                ("kmeans_pca", "word/media/image3.png"),
                ("ew_xgb", "word/media/image4.png"),
            ):
                if media_name in z.namelist():
                    out_p = tmp_dir / f"{key}.png"
                    out_p.write_bytes(z.read(media_name))
                    figs[key] = out_p

    # Vẽ hình Ma trận Chuyển dịch Cụm Markov 4x4 + Top 6 Luật kết hợp Apriori
    markov_apriori_path = tmp_dir / "markov_apriori.png"
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.6, 4.5), dpi=240)

    matrix = np.array(
        [
            [94.77, 3.15, 2.01, 0.07],
            [9.16, 84.40, 0.31, 6.14],
            [48.44, 43.49, 1.95, 6.12],
            [1.47, 16.12, 0.11, 82.30],
        ]
    )
    labels = ["Cụm 0\n(Thấp)", "Cụm 1\n(T.Bình)", "Cụm 2\n(Tăng vọt)", "Cụm 3\n(Rất cao)"]
    im = ax1.imshow(matrix, cmap="Blues", vmin=0, vmax=100)
    ax1.set_xticks(range(4))
    ax1.set_yticks(range(4))
    ax1.set_xticklabels(labels, fontsize=8.0)
    ax1.set_yticklabels(labels, fontsize=8.0)
    ax1.set_xlabel("Cụm ở tuần kế tiếp (t + 1)", fontsize=9.0, fontweight="bold")
    ax1.set_ylabel("Cụm ở tuần hiện tại (t)", fontsize=9.0, fontweight="bold")
    ax1.set_title("(a) Ma trận Xác suất Chuyển cụm Markov (%)", fontsize=10.0, fontweight="bold", pad=8)
    for i in range(4):
        for j in range(4):
            val = matrix[i, j]
            txt_color = "white" if val >= 50 else "#0F172A"
            ax1.text(j, i, f"{val:.1f}%", ha="center", va="center", color=txt_color, fontsize=8.5, fontweight="bold")

    rule_labels = [
        "Tăng huyết áp + 1 mũi\n⇒ Dương tính",
        "Tiểu đường + 40–59 tuổi\n⇒ Dương tính",
        "Tiểu đường + AstraZeneca\n⇒ Dương tính",
        "Chưa tiêm + ≥60 tuổi\n⇒ Dương tính",
        "1 mũi + 40–59 tuổi\n⇒ Dương tính",
        "Nam + 1 mũi + 40–59 tuổi\n⇒ Dương tính",
    ]
    lifts = [2.70, 2.40, 2.32, 2.26, 2.22, 2.16]
    confs = [73.8, 65.8, 63.4, 61.9, 60.8, 59.1]
    y_pos = np.arange(len(rule_labels))
    bars = ax2.barh(y_pos, list(reversed(lifts)), color="#0F766E", height=0.62)
    ax2.set_yticks(y_pos)
    ax2.set_yticklabels(list(reversed(rule_labels)), fontsize=7.8)
    ax2.set_xlim(0, 3.45)
    ax2.set_xlabel("Độ nâng (Lift) — Kèm Độ tin cậy (Confidence)", fontsize=8.8, fontweight="bold")
    ax2.set_title("(b) Top 6 Luật Kết hợp Nguy cơ Cao (Apriori)", fontsize=10.0, fontweight="bold", pad=8)
    ax2.grid(True, axis="x", linestyle="--", alpha=0.35)
    for bar, lift_v, conf_v in zip(bars, reversed(lifts), reversed(confs)):
        ax2.text(
            bar.get_width() + 0.05,
            bar.get_y() + bar.get_height() / 2,
            f"Lift {lift_v:.2f}x ({conf_v:.1f}%)",
            va="center",
            ha="left",
            fontsize=7.6,
            fontweight="bold",
            color="#0F172A",
        )

    fig.tight_layout(pad=1.0)
    fig.savefig(markov_apriori_path, dpi=240)
    plt.close(fig)
    figs["markov_apriori"] = markov_apriori_path
    return figs


# ==============================================================================
# 2. TIỆN ÍCH CHÈN ĐOẠN VĂN, BẢNG, ẢNH TRƯỚC MỘT PARAGRAPH MỐC TRONG DOCX
# ==============================================================================
def _set_run_font(run, size_pt=12.0, bold=False, italic=False, color=DARK):
    run.font.name = "Times New Roman"
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
        rFonts.set(qn(attr), "Times New Roman")


def insert_h2_before(anchor_p, text: str):
    p = anchor_p.insert_paragraph_before("")
    try:
        p.style = "Heading 2"
    except Exception:
        pass
    p.paragraph_format.space_before = Pt(11)
    p.paragraph_format.space_after = Pt(4)
    p.paragraph_format.keep_with_next = True
    r = p.add_run(text)
    _set_run_font(r, size_pt=13.0, bold=True, color=NAVY)
    return p


def insert_body_before(anchor_p, text: str, bold_prefix: str = None):
    p = anchor_p.insert_paragraph_before("")
    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    p.paragraph_format.line_spacing = 1.25
    p.paragraph_format.space_after = Pt(4.5)
    if bold_prefix:
        r0 = p.add_run(bold_prefix)
        _set_run_font(r0, size_pt=12.0, bold=True, color=DARK)
    r = p.add_run(text)
    _set_run_font(r, size_pt=12.0, color=DARK)
    return p


def insert_bullet_before(anchor_p, lead: str, text: str):
    p = anchor_p.insert_paragraph_before("")
    p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    p.paragraph_format.left_indent = Cm(0.75)
    p.paragraph_format.first_line_indent = Cm(-0.35)
    p.paragraph_format.line_spacing = 1.22
    p.paragraph_format.space_after = Pt(3.0)
    r_dot = p.add_run("•  ")
    _set_run_font(r_dot, size_pt=12.0, bold=True, color=NAVY)
    if lead:
        r_lead = p.add_run(lead)
        _set_run_font(r_lead, size_pt=12.0, bold=True, color=DARK)
    r_txt = p.add_run(text)
    _set_run_font(r_txt, size_pt=12.0, color=DARK)
    return p


def insert_figure_before(anchor_p, img_path: Path, caption: str, width_in: float = 6.2):
    p_img = anchor_p.insert_paragraph_before("")
    p_img.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_img.paragraph_format.space_before = Pt(5)
    p_img.paragraph_format.space_after = Pt(2)
    p_img.paragraph_format.keep_with_next = True
    p_img.add_run().add_picture(str(img_path), width=Inches(width_in))

    p_cap = anchor_p.insert_paragraph_before("")
    p_cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_cap.paragraph_format.space_after = Pt(6)
    r = p_cap.add_run(caption)
    _set_run_font(r, size_pt=10.5, italic=True, color=SLATE)


def insert_table_before(doc: Document, anchor_p, headers: list[str], rows: list[list[str]], highlight_row: int = None):
    tbl = doc.add_table(rows=len(rows) + 1, cols=len(headers))
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    tblPr = tbl._tbl.tblPr
    tblPr.append(
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
    for c_idx, h_txt in enumerate(headers):
        cell = tbl.rows[0].cells[c_idx]
        cell._tc.get_or_add_tcPr().append(parse_xml(f'<w:shd {nsdecls("w")} w:fill="1E3A8A"/>'))
        cell.vertical_alignment = WD_CELL_VERTICAL.CENTER
        p = cell.paragraphs[0]
        p.paragraph_format.space_after = Pt(0)
        r = p.add_run(h_txt)
        _set_run_font(r, size_pt=10.5, bold=True, color=RGBColor(0xFF, 0xFF, 0xFF))

    for r_idx, row_vals in enumerate(rows):
        is_hl = highlight_row is not None and r_idx == highlight_row
        fill = "DBEAFE" if is_hl else ("F8FAFC" if r_idx % 2 == 1 else "FFFFFF")
        for c_idx, val in enumerate(row_vals):
            cell = tbl.rows[r_idx + 1].cells[c_idx]
            cell._tc.get_or_add_tcPr().append(parse_xml(f'<w:shd {nsdecls("w")} w:fill="{fill}"/>'))
            cell.vertical_alignment = WD_CELL_VERTICAL.CENTER
            p = cell.paragraphs[0]
            p.paragraph_format.space_after = Pt(0)
            r = p.add_run(str(val))
            _set_run_font(r, size_pt=10.5, bold=is_hl, color=DARK)

    # Di chuyển bảng lên ngay trước anchor_p
    anchor_p._p.addprevious(tbl._tbl)
    sp = anchor_p.insert_paragraph_before("")
    sp.paragraph_format.space_after = Pt(3)
    return tbl


def find_paragraph_by_prefix(doc: Document, prefix: str, start_idx: int = 0):
    for idx in range(start_idx, len(doc.paragraphs)):
        if doc.paragraphs[idx].text.strip().startswith(prefix):
            return idx, doc.paragraphs[idx]
    return -1, None


def update_baocao_docx(figs: dict[str, Path]) -> None:
    doc = Document(str(BAOCAO_DOCX))

    # Nếu đã có 6.11–6.14 nhưng chưa có 6.15 (Isolation Forest + TreeSHAP + AI Portal), bổ sung phần nâng cấp v3
    if any("6.11. Nâng cấp K-Means" in p.text for p in doc.paragraphs):
        if any("6.15. Phát hiện Bất thường" in p.text for p in doc.paragraphs):
            print("[!] baocao-datamining.docx đã có đầy đủ đến mục 6.15 và 7.7.")
            return

        _, ch7 = find_paragraph_by_prefix(doc, "CHƯƠNG 7:", start_idx=120)
        if ch7 is not None:
            insert_h2_before(ch7, "6.15. Phát hiện Bất thường Kép (Isolation Forest + Kafka Realtime) & Giải thích TreeSHAP")
            insert_body_before(
                ch7,
                "Hoàn thiện trụ cột thứ 5 của Khai phá dữ liệu, hệ thống bổ sung đồng thời hai cơ chế phát hiện bất thường (Batch và Streaming) kết hợp giải thích mô hình cục bộ TreeSHAP cho từng quốc gia:",
            )
            insert_bullet_before(ch7, "Phát hiện bất thường lịch sử bằng Isolation Forest (airflow/dags/covid_anomaly_detection.py): ", "Phân tích 5 đặc trưng dịch tễ (log_cases, log_deaths, case_z_score, death_z_score, cfr_weekly) trên 56.477 quan sát, phát hiện 1.130 tuần bất thường gồm: Nhiễu dồn/hiệu chỉnh báo cáo (570), Tử vong tăng dị thường (532) và Đột biến ca mắc cục bộ (28), lưu tại dwh.fact_covid_anomalies.")
            insert_bullet_before(ch7, "Phát hiện ổ dịch thời gian thực trên Kafka CDC (cdc/consumer.py): ", "Lớp StreamingOutbreakDetector theo dõi cửa sổ trượt 300s các ca CONFIRMED_COVID theo tỉnh; thử nghiệm mô phỏng ổ dịch tại Hà Nội kích hoạt cảnh báo đỏ sau < 3 giây (Z-score = 4,95) vào dwh.fact_realtime_anomalies.")
            insert_bullet_before(ch7, "Giải thích cục bộ TreeSHAP (top_reasons_vi): ", "Tính đóng góp SHAP chính xác bằng pred_contribs=True của XGBoost cho từng quốc gia (VD: Romania 85,8% do Tốc độ tăng 2 tuần +0,58 và Tốc độ tăng 1 tuần +0,54), hiển thị trực tiếp trên Dashboard 3 Superset và AI Portal.")

        _, ch8 = find_paragraph_by_prefix(doc, "CHƯƠNG 8:", start_idx=120)
        if ch8 is not None:
            insert_h2_before(ch8, "7.7. Cổng Web Tương tác AI & Mô phỏng Kịch bản What-If (Streamlit Portal — Port 8501)")
            insert_body_before(
                ch8,
                "Bổ trợ cho 4 Dashboard BI trên Superset (đã mở rộng lên 17 Datasets và 28 biểu đồ tiếng Việt), nhóm triển khai container thứ 11 (ai-portal, portal/app.py) tại http://localhost:8501 gồm 4 phân hệ tương tác trực tiếp: (1) Mô phỏng What-If trên mô hình XGBoost thật kèm biểu đồ giải thích TreeSHAP; (2) Tra cứu nguy cơ bệnh nhân khớp trực tiếp tập luật Apriori; (3) Quỹ đạo quốc gia kèm điểm bất thường Isolation Forest & dự phóng Markov P^k; (4) Giám sát biểu ngữ cảnh báo ổ dịch thời gian thực từ Kafka CDC. Tổng bộ kiểm thử đơn vị tự động nâng lên 28/28 Unit Tests PASS (11 CDC + 17 ML).",
            )

        doc.save(str(BAOCAO_DOCX))
        print(f"[+] Đã bổ sung mục 6.15 & 7.7 vào baocao-datamining.docx ({BAOCAO_DOCX.stat().st_size:,} bytes)")
        return

    # 1. Cập nhật Mục lục (các dòng Normal trong đoạn 0..111)
    toc_additions = [
        ("CHƯƠNG 5:", "4.9. Mở rộng lược đồ DWH cho Dữ liệu Bệnh nhân Ẩn danh & 4 Mô hình Data Mining"),
        ("CHƯƠNG 6:", "5.7. Mở rộng CDC v2: Đồng bộ ẩn danh bảng bệnh nhân (dim_patient) & Placeholder"),
        ("CHƯƠNG 7:", "6.11. Nâng cấp K-Means: Đánh giá định lượng K = 2..8, Hồ sơ cụm tự sinh & PCA 2D"),
        ("CHƯƠNG 7:", "6.12. Khai phá Chuỗi Chuyển dịch Cụm Markov & Thời gian Lưu trú (Dwell Time)"),
        ("CHƯƠNG 7:", "6.13. Hệ thống Cảnh báo Sớm Bùng phát Dịch bằng XGBoost (Purged Time Split)"),
        ("CHƯƠNG 7:", "6.14. Khai phá Luật Kết hợp Bệnh nhân & Tiêm chủng CDC (Apriori / FP-Growth)"),
        ("CHƯƠNG 8:", "7.6. Tự động hóa khởi tạo & Việt hóa 4 Dashboard (15 Datasets, 25 Biểu đồ) qua REST API"),
        ("CHƯƠNG 9:", "8.6. Bộ kiểm thử đơn vị tự động (26/26 Unit Tests) & Kiểm định chống rò rỉ"),
    ]
    for before_prefix, toc_line in toc_additions:
        for idx in range(25, 115):
            if idx < len(doc.paragraphs) and doc.paragraphs[idx].text.strip().startswith(before_prefix):
                p = doc.paragraphs[idx].insert_paragraph_before("")
                p.paragraph_format.left_indent = Cm(0.5)
                p.paragraph_format.space_after = Pt(1.5)
                r = p.add_run(toc_line)
                _set_run_font(r, size_pt=11.0, color=SLATE)
                break

    # 2. Chèn mục 4.9 ngay trước CHƯƠNG 5 (thân bài)
    _, ch5 = find_paragraph_by_prefix(doc, "CHƯƠNG 5:", start_idx=120)
    if ch5 is not None:
        insert_h2_before(ch5, "4.9. Mở rộng lược đồ DWH cho Bệnh nhân Ẩn danh và 4 Mô hình Data Mining")
        insert_body_before(
            ch5,
            "Nhằm đáp ứng yêu cầu mở rộng từ một bài toán phân cụm đơn lẻ sang nền tảng Khai phá dữ liệu đa mô hình, kịch bản migration idempotent scripts/sql/upgrade-dwh.sql bổ sung 9 bảng phân tích mới và 15 khung nhìn (Views) có sẵn các cột nhãn tiếng Việt (*_vi) phục vụ trực tiếp tầng Apache Superset:",
        )
        insert_table_before(
            doc,
            ch5,
            headers=["Bảng / View mới trong schema dwh", "Loại", "Mục đích lưu trữ & phân tích"],
            rows=[
                ["dim_patient / vw_dashboard_patients", "Dimension", "Lưu hồ sơ bệnh nhân đã ẩn danh (loại bỏ full_name, citizen_hash), hỗ trợ is_placeholder."],
                ["fact_vaccination.patient_id / vw_vaccination_by_patient", "Fact + FK", "Liên kết mũi tiêm với nhân khẩu học (nhóm tuổi, giới tính, bệnh nền)."],
                ["fact_kmeans_evaluation & fact_kmeans_cluster_profile", "Data Mining", "Lưu 4 chỉ số đánh giá K = 2..8 và hồ sơ thống kê + nhãn tiếng Việt từng cụm."],
                ["fact_weekly_covid_cluster (+cluster_label, pca_1, pca_2)", "Data Mining", "Bổ sung nhãn cụm tự sinh và tọa độ giảm chiều PCA 2 chiều cho 56.477 quan sát."],
                ["fact_cluster_transition & fact_cluster_dwell_time", "Markov", "Lưu ma trận xác suất chuyển cụm tuần kế tiếp và thời gian lưu trú trung bình mỗi đợt."],
                ["fact_early_warning (+evaluation, +feature_importance)", "XGBoost", "Lưu xác suất cảnh báo trước 2 tuần (test/forecast), so sánh 3 mô hình & độ quan trọng biến."],
                ["fact_patient_association_rules / vw_patient_association_rules", "Apriori", "Lưu Top 40 luật kết hợp giữa tuổi, bệnh nền, số mũi tiêm và tình trạng COVID-19."],
            ],
        )

    # 3. Chèn mục 5.7 ngay trước CHƯƠNG 6
    _, ch6 = find_paragraph_by_prefix(doc, "CHƯƠNG 6:", start_idx=120)
    if ch6 is not None:
        insert_h2_before(ch6, "5.7. Mở rộng CDC v2: Đồng bộ ẩn danh bảng bệnh nhân (dim_patient) & Cơ chế Placeholder")
        insert_body_before(
            ch6,
            "Để khắc phục hạn chế Debezium không tự chụp lại snapshot cho bảng mới khi thêm vào connector đã có offset, hệ thống nâng cấp lên connector vaccination-oltp-connector-v2 (server.id = 184055, topic schema history riêng) đồng thời lập trình hàm remove_legacy_connectors() tự động gỡ connector cũ tránh nhân đôi sự kiện. Bộ tiêu thụ cdc/consumer.py lắng nghe đủ 3 bảng (vaccination_centers, patients, vaccination_records):",
        )
        insert_bullet_before(ch6, "Tối thiểu hóa dữ liệu cá nhân (Privacy by Design): ", "Cắt bỏ hoàn toàn cột full_name và citizen_hash trước khi ghi sang dwh.dim_patient; tương tự nhánh batch extract_mysql_oltp cũng lược bỏ full_name trước khi đẩy lên MinIO.")
        insert_bullet_before(ch6, "Cơ chế Placeholder cho sự kiện lệch thứ tự: ", "Khi bản ghi vaccination_records trên Kafka tới trước bản ghi patients, consumer tự động tạo trước dòng dim_patient(is_placeholder = TRUE) để bảo toàn khóa ngoại, sau đó ghi đè đầy đủ thuộc tính khi bản tin bệnh nhân tới.")
        insert_bullet_before(ch6, "Dữ liệu vận hành quy mô lớn: ", "Xây dựng script scripts/seed_oltp_data.py nạp 1.500+ bệnh nhân và 3.257+ mũi tiêm trải đều 10 tỉnh/thành, các nhóm tuổi và bệnh nền, đồng bộ tức thì sang DWH qua Debezium + Kafka.")

        # Cập nhật tiêu đề Chương 6 phản ánh đủ 4 mô hình Data Mining
        ch6.text = "CHƯƠNG 6: KHAI PHÁ DỮ LIỆU ĐA MÔ HÌNH (K-MEANS/PCA, MARKOV, XGBOOST & APRIORI)"

    # 4. Chèn các mục 6.11 -> 6.14 ngay trước CHƯƠNG 7
    _, ch7 = find_paragraph_by_prefix(doc, "CHƯƠNG 7:", start_idx=120)
    if ch7 is not None:
        # 6.11 K-Means v2
        insert_h2_before(ch7, "6.11. Nâng cấp K-Means: Đánh giá định lượng K = 2..8, Hồ sơ cụm tự sinh & Giảm chiều PCA 2D")
        insert_body_before(
            ch7,
            "Thay vì chọn cố định K = 4 theo cảm tính, module airflow/dags/weekly_covid_kmeans.py được tái cấu trúc để đánh giá toàn bộ dải K = 2..8 bằng 4 thước đo độc lập (Inertia/Elbow, Silhouette trên mẫu 10.000 điểm, Davies–Bouldin và Calinski–Harabasz), hỗ trợ chế độ tự chọn COVID_KMEANS_CLUSTERS=auto, tự sinh nhãn dịch tễ tiếng Việt từ tâm cụm giải chuẩn hóa (scaler.inverse_transform) và chiếu PCA 2 chiều (pca_1, pca_2):",
        )
        insert_table_before(
            doc,
            ch7,
            headers=["Số cụm K", "Inertia (WCSS)", "Silhouette (↑)", "Davies–Bouldin (↓)", "Calinski–Harabasz (↑)", "Trạng thái"],
            rows=[
                ["2", "100.175", "0,5713", "0,8873", "39.044,6", "—"],
                ["3", "75.258", "0,5393", "0,8937", "35.333,8", "—"],
                ["4", "57.580", "0,5478", "0,7372", "36.566,5", "Đang chọn (Mặc định)"],
                ["5", "40.018", "0,5821", "0,6720", "45.656,5", "Đỉnh Silhouette (Auto)"],
                ["6", "34.102", "0,5560", "0,7877", "44.820,2", "—"],
                ["7", "29.481", "0,5663", "0,8073", "44.678,2", "—"],
                ["8", "26.126", "0,5335", "0,8567", "44.248,9", "—"],
            ],
            highlight_row=2,
        )
        if "kmeans_pca" in figs:
            insert_figure_before(
                ch7,
                figs["kmeans_pca"],
                "Hình 6.3. Đánh giá số cụm K = 2..8 (Elbow & Silhouette) và phân tách 4 cụm trên mặt phẳng PCA 2 chiều.",
            )

        # 6.12 Markov
        insert_h2_before(ch7, "6.12. Khai phá Chuỗi Chuyển dịch Cụm Markov & Thời gian Lưu trú (Dwell Time)")
        insert_body_before(
            ch7,
            "Module airflow/dags/covid_cluster_transitions.py khai phá động lực học dịch tễ trên chuỗi tuần liên tiếp của 56.477 quan sát thông qua Ma trận chuyển trạng thái Markov bậc 1 P(C_{t+1} = j | C_t = i) và độ dài đợt lưu trú liên tục (Dwell Time):",
        )
        insert_table_before(
            doc,
            ch7,
            headers=["Cụm", "Nhãn cụm tự sinh", "Số đợt", "Lưu trú TB / đợt", "Đợt dài nhất", "Xác suất giữ cụm"],
            rows=[
                ["Cụm 0", "Thấp – ổn định", "2.054", "17,15 tuần", "240 tuần", "94,77%"],
                ["Cụm 1", "Trung bình – ổn định", "2.336", "6,38 tuần", "109 tuần", "84,40%"],
                ["Cụm 2", "Cao – đang tăng", "755", "1,02 tuần", "2 tuần", "1,95%"],
                ["Cụm 3", "Rất cao – ổn định", "986", "5,65 tuần", "75 tuần", "82,30%"],
            ],
        )
        insert_body_before(
            ch7,
            "Phân tích cho thấy Cụm 2 (Cao – đang tăng) là trạng thái bùng phát chuyển tiếp điển hình (trung bình chỉ kéo dài 1,02 tuần), trong khi một khi quốc gia đã rơi vào Cụm 3 (Rất cao – ổn định), đợt khủng hoảng y tế kéo dài trung bình 5,65 tuần liên tiếp (xác suất duy trì 82,30%) và mỗi tuần chỉ có 16,12% cơ hội hạ nhiệt về Cụm 1.",
            bold_prefix="Phát hiện dịch tễ nổi bật: ",
        )

        # 6.13 XGBoost Early Warning
        insert_h2_before(ch7, "6.13. Hệ thống Cảnh báo Sớm Bùng phát Dịch bằng XGBoost (Supervised Classification)")
        insert_body_before(
            ch7,
            "Module airflow/dags/covid_early_warning.py xây dựng bài toán phân lớp nhị phân cảnh báo sớm trước H = 2 tuần: gán nhãn bùng phát (surge = 1) khi tỷ lệ ca mắc sau 2 tuần tăng tối thiểu 1,5 lần và đạt từ 10 ca/100.000 dân trở lên. Mô hình sử dụng 18 đặc trưng nhân quả (lag 1–3 tuần, trung bình/độ lệch chuẩn trượt 4 tuần, vận tốc & gia tốc tăng trưởng, CFR trượt 4 tuần, độ phủ tiêm chủng cummax/dân số, mùa vụ sin/cos, bối cảnh toàn cầu và cluster_id từ K-Means). Để chống rò rỉ dữ liệu chuỗi thời gian, tập huấn luyện (44.520 dòng) và kiểm thử (11.006 dòng) được ngăn cách bởi khoảng trống purge H = 2 tuần; ngưỡng quyết định tối ưu theo F1 trên tập validation và xử lý mất cân bằng lớp bằng scale_pos_weight.",
        )
        insert_table_before(
            doc,
            ch7,
            headers=["Mô hình đối sánh (Tập Test = 11.006 dòng)", "Ngưỡng", "Precision", "Recall", "F1-Score", "ROC-AUC", "PR-AUC"],
            rows=[
                ["XGBoost (Mô hình chính)", "0,64", "0,244", "0,663", "0,356", "0,925", "0,270"],
                ["Hồi quy Logistic (class_weight=balanced)", "0,40", "0,173", "0,750", "0,282", "0,876", "0,165"],
                ["Luật cơ sở (Tăng tuần trước ≥ 25%)", "0,50", "0,104", "0,347", "0,161", "0,728", "0,070"],
                ["XGBoost Ablation (Bỏ biến cluster_id)", "0,62", "0,239", "0,694", "0,355", "0,926", "0,264"],
            ],
            highlight_row=0,
        )
        if "ew_xgb" in figs:
            insert_figure_before(
                ch7,
                figs["ew_xgb"],
                "Hình 6.4. Đối sánh hiệu năng 3 mô hình cảnh báo sớm trên tập kiểm thử độc lập và Top 10 đặc trưng quan trọng nhất.",
            )

        # 6.14 Association Rules
        insert_h2_before(ch7, "6.14. Khai phá Luật Kết hợp trên Hồ sơ Bệnh nhân & Tiêm chủng CDC (Apriori / FP-Growth)")
        insert_body_before(
            ch7,
            "Module airflow/dags/patient_association_rules.py (task patient_association_rules trong DAG Airflow) chuyển đổi 1.508 hồ sơ bệnh nhân ẩn danh kết hợp lịch sử mũi tiêm thành tập giao dịch 5 thuộc tính rời rạc và khai phá các luật kết hợp {Nhóm tuổi, Giới tính, Bệnh nền, Trạng thái tiêm chủng, Loại vắc-xin} ⇒ {Kết cục sức khỏe}:",
        )
        insert_table_before(
            doc,
            ch7,
            headers=["#", "Tổ hợp điều kiện (Vế trái)", "Kết cục (Vế phải)", "Số BN", "Support", "Confidence", "Lift"],
            rows=[
                ["1", "Bệnh nền: Tăng huyết áp + Tiêm chủng: Mới tiêm 1 mũi", "Dương tính COVID-19", "31", "2,06%", "73,81%", "2,70x"],
                ["2", "Bệnh nền: Tiểu đường + Tuổi: 40 – 59", "Dương tính COVID-19", "25", "1,66%", "65,79%", "2,40x"],
                ["3", "Bệnh nền: Tiểu đường + Vắc-xin: AstraZeneca", "Dương tính COVID-19", "26", "1,72%", "63,41%", "2,32x"],
                ["4", "Tiêm chủng: Chưa tiêm (0 mũi) + Tuổi: Từ 60 trở lên", "Dương tính COVID-19", "26", "1,72%", "61,90%", "2,26x"],
                ["5", "Tiêm chủng: Mới tiêm 1 mũi + Tuổi: 40 – 59", "Dương tính COVID-19", "48", "3,18%", "60,76%", "2,22x"],
                ["6", "Giới tính: Nam + Tiêm chủng: Mới tiêm 1 mũi + Tuổi: 40 – 59", "Dương tính COVID-19", "26", "1,72%", "59,09%", "2,16x"],
            ],
            highlight_row=0,
        )
        if "markov_apriori" in figs:
            insert_figure_before(
                ch7,
                figs["markov_apriori"],
                "Hình 6.5. Ma trận xác suất chuyển dịch cụm Markov 4×4 và Top 6 luật kết hợp nguy cơ cao trên dữ liệu bệnh nhân CDC.",
            )

    # 5. Chèn mục 7.6 ngay trước CHƯƠNG 8
    _, ch8 = find_paragraph_by_prefix(doc, "CHƯƠNG 8:", start_idx=120)
    if ch8 is not None:
        insert_h2_before(ch8, "7.6. Tự động hóa khởi tạo & Việt hóa 4 Dashboard (15 Datasets, 25 Biểu đồ) qua REST API")
        insert_body_before(
            ch8,
            "Nhóm xây dựng kịch bản scripts/init_superset_dashboards.py giao tiếp trực tiếp với Apache Superset REST API (/api/v1/...) bằng thư viện chuẩn Python, thực hiện đăng ký/đồng bộ cột cho toàn bộ 15 Dataset Views, gán từ điển tên hiển thị tiếng Việt (verbose_name) và xuất bản 4 Dashboard tiếng Việt hoàn chỉnh (đã kiểm định 25/25 biểu đồ chạy không lỗi):",
        )
        insert_table_before(
            doc,
            ch8,
            headers=["STT", "Tên Dashboard Tiếng Việt trên Superset", "Slug đường dẫn", "Số biểu đồ & Trọng tâm hiển thị"],
            rows=[
                ["1", "Tổng quan Phân cụm Dịch tễ COVID-19 Toàn cầu", "covid19-kmeans-clusters", "4 biểu đồ: Tương quan ca mắc – tử vong, tỷ trọng cụm, bảng tổng hợp 4 nhóm."],
                ["2", "Phân tích Phân cụm — Đánh giá K, PCA & Chuyển cụm Markov", "covid19-kmeans-mining", "8 biểu đồ: Elbow Inertia, Silhouette/DB, PCA 2D, hồ sơ cụm, Ma trận Markov & Dwell Time."],
                ["3", "Hệ thống Cảnh báo Sớm Bùng phát (XGBoost + TreeSHAP) & Isolation Forest", "covid19-early-warning", "7 biểu đồ: Bản đồ thế giới dự báo 2 tuần tới, đối sánh 3 mô hình, Top 10 đặc trưng, bảng xếp hạng TreeSHAP, Donut & bảng Isolation Forest."],
                ["4", "Giám sát Thời gian thực & Khai phá Luật Kết hợp (CDC)", "covid19-cdc-patients-vaccination", "9 biểu đồ: KPI thời gian thực, ca bệnh theo tuổi/bệnh nền/vắc-xin, thống kê Tỉnh / Thành phố, Luật Apriori & bảng cảnh báo ổ dịch Kafka."],
            ],
        )

    # 6. Chèn mục 8.6 ngay trước CHƯƠNG 9
    _, ch9 = find_paragraph_by_prefix(doc, "CHƯƠNG 9:", start_idx=120)
    if ch9 is not None:
        insert_h2_before(ch9, "8.6. Bộ kiểm thử đơn vị tự động (28/28 Unit Tests) & Kiểm định chống rò rỉ")
        insert_body_before(
            ch9,
            "Bên cạnh các kịch bản kiểm thử tích hợp end-to-end (TC01–TC08), hệ thống bổ sung bộ 28 Unit Tests tự động chạy định kỳ trên Python & container Airflow, đảm bảo 100% pass:",
        )
        insert_table_before(
            doc,
            ch9,
            headers=["Tệp kiểm thử đơn vị", "Số test", "Phạm vi kiểm định cốt lõi", "Kết quả"],
            rows=[
                ["cdc/test_consumer.py", "11", "Giải mã Debezium, ẩn danh PII bệnh nhân, Placeholder, gỡ connector cũ, rollback lỗi DWH & phát hiện ổ dịch Kafka.", "11/11 PASS"],
                ["airflow/tests/test_weekly_covid_kmeans.py", "8", "Chuẩn hóa dân số, đánh giá K = 2..8, chế độ auto-K theo Silhouette, thứ tự nhãn cụm & PCA.", "8/8 PASS"],
                ["airflow/tests/test_covid_early_warning.py", "5", "Gán nhãn surge, bất biến nhân quả (không rò rỉ t+1), Purged Split H tuần, an toàn đơn lớp.", "5/5 PASS"],
                ["airflow/tests/test_advanced_data_mining.py", "4", "Tổng xác suất Markov = 1, tính độ dài đợt Dwell Time, phát hiện luật Apriori Lift cao & Isolation Forest.", "4/4 PASS"],
            ],
            highlight_row=None,
        )

    # 7. Bổ sung tổng kết 5 trụ cột Data Mining vào cuối mục 9.1 (trước 9.2)
    _, sec92 = find_paragraph_by_prefix(doc, "9.2.", start_idx=120)
    if sec92 is not None:
        insert_bullet_before(
            sec92,
            "Hoàn thiện trọn bộ 5 trụ cột Khai phá dữ liệu trên cùng một Data Platform: ",
            "Phân cụm K-Means nâng cao + PCA 2D, Phân lớp Cảnh báo sớm XGBoost (ROC-AUC 0,925, PR-AUC 0,270, kiểm chứng Purged Split & Ablation), Khai phá Chuỗi chuyển dịch cụm Markov & Dwell Time, Khai phá Luật kết hợp Apriori trên bệnh nhân CDC, và Phát hiện bất thường kép (Isolation Forest + Kafka Realtime) kèm giải thích TreeSHAP.",
        )
        insert_bullet_before(
            sec92,
            "Tự động hóa 100% tầng trình diễn & kiểm thử: ",
            "Khởi tạo tự động 17 Dataset, 28 biểu đồ trên 4 Dashboard Superset kết hợp Cổng Web AI Streamlit (:8501), đi kèm 28/28 Unit Tests tự động.",
        )

    doc.save(str(BAOCAO_DOCX))
    print(f"[+] Đã cập nhật hoàn chỉnh báo cáo Word: {BAOCAO_DOCX} ({BAOCAO_DOCX.stat().st_size:,} bytes)")


# ==============================================================================
# 3. CẬP NHẬT & MỞ RỘNG SLIDE POWERPOINT (baocao/slide-datamining.pptx: 17 -> 21 SLIDES)
# ==============================================================================
def _add_styled_slide_base(prs: Presentation, title_text: str):
    blank_layout = prs.slide_layouts[6] if len(prs.slide_layouts) > 6 else prs.slide_layouts[-1]
    slide = prs.slides.add_slide(blank_layout)

    # Nền đồng nhất #F8FAFC
    bg = slide.background
    fill = bg.fill
    fill.solid()
    fill.fore_color.rgb = PptRGB(0xF8, 0xFA, 0xFC)

    # Khối tiêu đề đầu trang (khớp pixel-for-pixel với slide12..17 gốc)
    tb = slide.shapes.add_textbox(PptIn(0.6), PptIn(0.32), PptIn(12.13), PptIn(0.95))
    tf = tb.text_frame
    tf.word_wrap = True
    p1 = tf.paragraphs[0]
    r1 = p1.add_run()
    r1.text = "NỀN TẢNG DỮ LIỆU ĐA NGUỒN & KHAI PHÁ DỮ LIỆU COVID-19"
    r1.font.name = "Arial"
    r1.font.size = PptPt(10.5)
    r1.font.bold = True
    r1.font.color.rgb = PptRGB(0x0F, 0x76, 0x6E)

    p2 = tf.add_paragraph()
    p2.space_before = PptPt(2)
    r2 = p2.add_run()
    r2.text = title_text
    r2.font.name = "Arial"
    r2.font.size = PptPt(21.5)
    r2.font.bold = True
    r2.font.color.rgb = PptRGB(0x0F, 0x17, 0x2A)

    # Đường kẻ ngang #CBD5E1
    bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, PptIn(0.6), PptIn(1.32), PptIn(12.13), PptIn(0.03))
    bar.fill.solid()
    bar.fill.fore_color.rgb = PptRGB(0xCB, 0xD5, 0xE1)
    bar.line.fill.background()

    return slide


def _add_white_card(slide, left, top, width, height, header_text: str, bullets: list[str], accent_rgb=PptRGB(0x1E, 0x3A, 0x8A)):
    card = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, PptIn(left), PptIn(top), PptIn(width), PptIn(height))
    card.fill.solid()
    card.fill.fore_color.rgb = PptRGB(0xFF, 0xFF, 0xFF)
    card.line.color.rgb = PptRGB(0xE2, 0xE8, 0xF0)
    card.line.width = PptPt(1.0)

    tb = slide.shapes.add_textbox(PptIn(left + 0.18), PptIn(top + 0.14), PptIn(width - 0.36), PptIn(height - 0.25))
    tf = tb.text_frame
    tf.word_wrap = True
    p0 = tf.paragraphs[0]
    r0 = p0.add_run()
    r0.text = header_text
    r0.font.name = "Arial"
    r0.font.size = PptPt(13.5)
    r0.font.bold = True
    r0.font.color.rgb = accent_rgb

    for item in bullets:
        p = tf.add_paragraph()
        p.space_before = PptPt(6)
        r = p.add_run()
        r.text = f"•  {item}"
        r.font.name = "Arial"
        r.font.size = PptPt(11.5)
        r.font.color.rgb = PptRGB(0x1E, 0x29, 0x3B)


def _add_ppt_table(slide, left, top, width, height, headers: list[str], rows: list[list[str]], highlight_row: int = None):
    tbl_shape = slide.shapes.add_table(len(rows) + 1, len(headers), PptIn(left), PptIn(top), PptIn(width), PptIn(height))
    tbl = tbl_shape.table
    for c_idx, h in enumerate(headers):
        cell = tbl.cell(0, c_idx)
        cell.fill.solid()
        cell.fill.fore_color.rgb = PptRGB(0x1E, 0x3A, 0x8A)
        cell.vertical_anchor = MSO_ANCHOR.MIDDLE
        p = cell.text_frame.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        r = p.add_run()
        r.text = h
        r.font.name = "Arial"
        r.font.size = PptPt(10.5)
        r.font.bold = True
        r.font.color.rgb = PptRGB(0xFF, 0xFF, 0xFF)

    for r_idx, r_vals in enumerate(rows):
        is_hl = highlight_row is not None and r_idx == highlight_row
        bg = PptRGB(0xDB, 0xEA, 0xFE) if is_hl else (PptRGB(0xF8, 0xFA, 0xFC) if r_idx % 2 == 1 else PptRGB(0xFF, 0xFF, 0xFF))
        for c_idx, val in enumerate(r_vals):
            cell = tbl.cell(r_idx + 1, c_idx)
            cell.fill.solid()
            cell.fill.fore_color.rgb = bg
            cell.vertical_anchor = MSO_ANCHOR.MIDDLE
            p = cell.text_frame.paragraphs[0]
            p.alignment = PP_ALIGN.LEFT if c_idx <= 1 else PP_ALIGN.CENTER
            r = p.add_run()
            r.text = str(val)
            r.font.name = "Arial"
            r.font.size = PptPt(10.0)
            r.font.bold = is_hl
            r.font.color.rgb = PptRGB(0x0F, 0x17, 0x2A)


def _move_slide(prs: Presentation, old_index: int, new_index: int):
    sldIdLst = prs.slides._sldIdLst
    slides = list(sldIdLst)
    el = slides[old_index]
    sldIdLst.remove(el)
    sldIdLst.insert(new_index, el)


def update_slide_pptx(figs: dict[str, Path]) -> None:
    prs = Presentation(str(SLIDE_PPTX))

    # Kiểm tra tránh chèn lặp nếu đã chạy trước đó
    all_texts = " ".join(
        sh.text for sl in prs.slides for sh in sl.shapes if sh.has_text_frame
    )
    if "Purged Split" in all_texts and "Luật Kết hợp" in all_texts:
        if "Cổng Web AI Tương tác" in all_texts:
            print("[!] slide-datamining.pptx đã có đầy đủ 22 slides (gồm AI Portal + Isolation Forest).")
            return

        # Thêm Slide mới về Isolation Forest, TreeSHAP, Kafka Realtime Anomaly & Streamlit AI Portal
        orig_n = len(prs.slides)
        s_ai = _add_styled_slide_base(
            prs,
            "Phát hiện Bất thường (Isolation Forest + Kafka Realtime) & Cổng Web AI Tương tác",
        )
        _add_white_card(
            s_ai, 0.6, 1.55, 5.9, 5.45,
            "1. Phát hiện Bất thường Kép & Giải thích TreeSHAP",
            [
                "Isolation Forest (Batch): Quét 56.477 tuần–quốc gia, gắn cờ 1.130 điểm bất thường — Nhiễu dồn báo cáo (570), Tử vong tăng dị thường (532), Đột biến ca mắc cục bộ (28).",
                "Kafka Streaming Anomaly (Realtime): Giám sát cửa sổ trượt 300s các ca CONFIRMED_COVID theo tỉnh; mô phỏng ổ dịch Hà Nội phát cảnh báo đỏ trong < 3 giây (Z-score = 4,95).",
                "TreeSHAP cục bộ (top_reasons_vi): Tự động giải thích 3 yếu tố quyết định cảnh báo cho từng quốc gia (VD: Romania 85,8% do Tốc độ tăng 2 tuần +0,58 & 1 tuần +0,54).",
                "Tổng bộ kiểm thử tự động đạt 28/28 Unit Tests PASS (11 CDC + 17 ML).",
            ],
            accent_rgb=PptRGB(0xB9, 0x1C, 0x1C),
        )
        _add_white_card(
            s_ai, 6.83, 1.55, 5.9, 5.45,
            "2. Cổng Web AI Tương tác Streamlit (http://localhost:8501)",
            [
                "Container thứ 11 (ai-portal) cho phép hội đồng tương tác trực tiếp với mô hình học máy thật:",
                "Tab 1 — Mô phỏng What-If XGBoost + TreeSHAP: Kéo thanh trượt vắc-xin/tốc độ tăng ca để xem xác suất bùng phát & biểu đồ SHAP thay đổi tức thì.",
                "Tab 2 — Tra cứu Nguy cơ Bệnh nhân (Apriori): Nhập tuổi, bệnh nền, số mũi tiêm để khớp luật nguy cơ & khuyến nghị tiêm nhắc lại.",
                "Tab 3 — Quỹ đạo Quốc gia & Dự phóng Markov P^k (1–16 tuần) kèm dấu X bất thường Isolation Forest.",
                "Tab 4 — Biểu ngữ báo động đỏ Realtime từ Kafka CDC & bảng bất thường toàn cầu.",
            ],
            accent_rgb=PptRGB(0x0F, 0x76, 0x6E),
        )
        # Đặt ngay sau Slide 17 (index 16) -> vị trí index 17 (trước slide Superset)
        _move_slide(prs, orig_n, 17)
        prs.save(str(SLIDE_PPTX))
        print(f"[+] Đã bổ sung Slide 18 (AI Portal + Isolation Forest + Kafka Anomaly) vào slide-datamining.pptx ({len(prs.slides)} slides)")
        return

    orig_count = len(prs.slides)  # 17 slides

    # Cập nhật nội dung slide Superset (slide14, index=13) và slide Kết luận (slide17, index=16)
    if orig_count >= 17:
        # Slide 14 (index 13): Trực quan hóa Superset
        sl14 = prs.slides[13]
        for sh in sl14.shapes:
            if sh.has_text_frame and "HỆ THỐNG DASHBOARD" in sh.text:
                tf = sh.text_frame
                tf.clear()
                p0 = tf.paragraphs[0]
                r0 = p0.add_run()
                r0.text = "4 DASHBOARD TIẾNG VIỆT TRÊN APACHE SUPERSET (Port 8089)"
                r0.font.name = "Arial"
                r0.font.size = PptPt(13.0)
                r0.font.bold = True
                r0.font.color.rgb = PptRGB(0x1E, 0x3A, 0x8A)
                for line in [
                    "Khởi tạo & Việt hóa tự động 15 Datasets + 25 Biểu đồ qua REST API (init_superset_dashboards.py).",
                    "Dashboard 1 — Tổng quan Phân cụm COVID-19 Toàn cầu (4 Nhóm xu hướng dịch tễ).",
                    "Dashboard 2 — Đánh giá K = 2..8 (Elbow/Silhouette), Không gian PCA 2D & Ma trận Markov.",
                    "Dashboard 3 — Cảnh báo Sớm Bùng phát 2 Tuần tới (Bản đồ thế giới, Đối sánh 3 mô hình, Top 10 đặc trưng).",
                    "Dashboard 4 — Giám sát Thời gian thực CDC (Bệnh nhân ẩn danh, Tiêm chủng & Top 10 Luật kết hợp Apriori).",
                ]:
                    p = tf.add_paragraph()
                    p.space_before = PptPt(7)
                    r = p.add_run()
                    r.text = f"•  {line}"
                    r.font.name = "Arial"
                    r.font.size = PptPt(11.5)
                    r.font.color.rgb = PptRGB(0x1E, 0x29, 0x3B)

        # Slide 17 (index 16): Kết luận, Hạn chế & Hướng mở rộng
        sl17 = prs.slides[16]
        for sh in sl17.shapes:
            if not sh.has_text_frame:
                continue
            if "KẾT LUẬN ĐẠT ĐƯỢC" in sh.text:
                tf = sh.text_frame
                tf.clear()
                r0 = tf.paragraphs[0].add_run()
                r0.text = "KẾT LUẬN ĐẠT ĐƯỢC"
                r0.font.name = "Arial"
                r0.font.size = PptPt(13.0)
                r0.font.bold = True
                r0.font.color.rgb = PptRGB(0x0F, 0x76, 0x6E)
                for line in [
                    "Hoàn thiện Data Platform 10 containers (Batch ETL + Real-time CDC v2 ẩn danh PII bệnh nhân).",
                    "Trọn bộ 4 kỹ thuật Data Mining: K-Means/PCA, Cảnh báo sớm XGBoost (ROC-AUC 0,925), Chuyển cụm Markov & Luật kết hợp Apriori.",
                    "Tự động hóa 4 Dashboard tiếng Việt (25 biểu đồ) qua Superset REST API & 26/26 Unit Tests pass.",
                ]:
                    p = tf.add_paragraph()
                    p.space_before = PptPt(7)
                    r = p.add_run()
                    r.text = f"•  {line}"
                    r.font.name = "Arial"
                    r.font.size = PptPt(11.0)
                    r.font.color.rgb = PptRGB(0x1E, 0x29, 0x3B)
            elif "HƯỚNG MỞ RỘNG" in sh.text:
                tf = sh.text_frame
                tf.clear()
                r0 = tf.paragraphs[0].add_run()
                r0.text = "HƯỚNG MỞ RỘNG"
                r0.font.name = "Arial"
                r0.font.size = PptPt(13.0)
                r0.font.bold = True
                r0.font.color.rgb = PptRGB(0x1E, 0x3A, 0x8A)
                for line in [
                    "Mở rộng xử lý phân tán bằng Apache Spark / Flink cho dữ liệu hàng tỷ bản ghi.",
                    "Phát hiện bất thường dịch tễ thời gian thực (Isolation Forest / Online Streaming Anomaly).",
                    "Giải thích cảnh báo cá thể hóa từng quốc gia bằng TreeSHAP & dự báo định lượng số ca (Quantile Forecast).",
                ]:
                    p = tf.add_paragraph()
                    p.space_before = PptPt(7)
                    r = p.add_run()
                    r.text = f"•  {line}"
                    r.font.name = "Arial"
                    r.font.size = PptPt(11.0)
                    r.font.color.rgb = PptRGB(0x1E, 0x29, 0x3B)

    # Thêm 4 Slide mới chuyên sâu về 4 nâng cấp Data Mining & CDC (sẽ chèn vào ngay sau Slide 13)
    # --- SLIDE MỚI 1: K-Means Nâng cao (Kiểm định K = 2..8, Hồ sơ cụm tự sinh & PCA 2D) ---
    s_km = _add_styled_slide_base(prs, "Nâng cấp K-Means: Kiểm định Số cụm K = 2..8, Hồ sơ Cụm & PCA 2D")
    if "kmeans_pca" in figs:
        s_km.shapes.add_picture(str(figs["kmeans_pca"]), PptIn(0.6), PptIn(1.52), width=PptIn(12.1))
    _add_white_card(
        s_km, 0.6, 5.88, 12.13, 1.32,
        "Điểm nâng cấp cốt lõi trên 56.477 quan sát tuần – quốc gia",
        [
            "Kiểm định K = 2..8 bằng 4 chỉ số (Inertia, Silhouette, Davies–Bouldin, Calinski–Harabasz); hỗ trợ chế độ tự động COVID_KMEANS_CLUSTERS=auto (K=5 đạt đỉnh Silhouette 0,5821; K=4 giữ 4 vùng dịch tễ chuẩn).",
            "Giải chuẩn hóa tâm cụm để tự động sinh nhãn tiếng Việt (Thấp – ổn định, Trung bình – ổn định, Cao – đang tăng, Rất cao – ổn định) và chiếu mặt phẳng PCA 2 chiều.",
        ],
    )

    # --- SLIDE MỚI 2: Khai phá Chuỗi Chuyển dịch Cụm Markov & Thời gian Lưu trú ---
    s_mk = _add_styled_slide_base(prs, "Khai phá Chuỗi Chuyển dịch Cụm Markov & Thời gian Lưu trú (Dwell Time)")
    _add_ppt_table(
        s_mk, 0.6, 1.55, 12.13, 2.35,
        headers=["Mã cụm", "Nhãn cụm dịch tễ tự sinh", "Số đợt", "Lưu trú TB / đợt", "Đợt dài nhất", "Xác suất giữ cụm"],
        rows=[
            ["Cụm 0", "Thấp – ổn định (62,4%)", "2.054", "17,15 tuần", "240 tuần", "94,77%"],
            ["Cụm 1", "Trung bình – ổn định (26,4%)", "2.336", "6,38 tuần", "109 tuần", "84,40%"],
            ["Cụm 2", "Cao – đang tăng (1,4%)", "755", "1,02 tuần", "2 tuần", "1,95%"],
            ["Cụm 3", "Rất cao – ổn định (9,9%)", "986", "5,65 tuần", "75 tuần", "82,30%"],
        ],
        highlight_row=3,
    )
    _add_white_card(
        s_mk, 0.6, 4.15, 5.9, 2.95,
        "Ý nghĩa Dịch tễ từ Ma trận Markov P(C_{t+1} | C_t)",
        [
            "Cụm 2 (Cao – đang tăng) là trạng thái chuyển tiếp ngắn (TB chỉ 1,02 tuần/đợt), phản ánh tuần bùng phát đột biến.",
            "Từ Cụm 1 (Trung bình), xác suất leo thang lên Vùng Đỏ (Cụm 3) là 6,14% (914 lần), còn hạ nhiệt về Vùng Xanh là 9,16%.",
            "Khi đã rơi vào Vùng Đỏ (Cụm 3), khủng hoảng kéo dài trung bình 5,65 tuần liên tiếp (giữ cụm 82,30%), chỉ 16,12%/tuần lui về Cụm 1.",
        ],
        accent_rgb=PptRGB(0x0F, 0x76, 0x6E),
    )
    _add_white_card(
        s_mk, 6.83, 4.15, 5.9, 2.95,
        "Tích hợp vào Kiến trúc Data Platform",
        [
            "Module thuần airflow/dags/covid_cluster_transitions.py chạy nối tiếp ngay sau K-Means.",
            "Lưu tự động vào 2 bảng Fact mới: dwh.fact_cluster_transition & dwh.fact_cluster_dwell_time.",
            "Trực quan hóa ngay trên Dashboard 2 Superset (Biểu đồ chuyển trạng thái % & Bảng Dwell Time).",
        ],
    )

    # --- SLIDE MỚI 3: Cảnh báo Sớm Bùng phát Dịch bằng XGBoost ---
    s_ew = _add_styled_slide_base(prs, "Cảnh báo Sớm Bùng phát Dịch trước 2 Tuần bằng XGBoost (Purged Split)")
    if "ew_xgb" in figs:
        s_ew.shapes.add_picture(str(figs["ew_xgb"]), PptIn(0.6), PptIn(1.50), width=PptIn(12.1))
    _add_white_card(
        s_ew, 0.6, 5.80, 12.13, 1.40,
        "Kết quả kiểm định độc lập (44.520 mẫu Train / 11.006 mẫu Test — Khoảng cách Purge H = 2 tuần)",
        [
            "XGBoost vượt trội Hồi quy Logistic & Luật cơ sở: ROC-AUC = 0,925 | PR-AUC = 0,270 (gấp ~4 lần luật cơ sở 0,070) | F1 = 0,356.",
            "Kiểm chứng Ablation (tắt cluster_id): ROC-AUC = 0,926, PR-AUC = 0,264, F1 = 0,355 — khẳng định không bị rò rỉ dữ liệu từ tâm cụm K-Means.",
        ],
        accent_rgb=PptRGB(0xB9, 0x1C, 0x1C),
    )

    # --- SLIDE MỚI 4: CDC Ẩn danh Bệnh nhân & Khai phá Luật Kết hợp Apriori ---
    s_ar = _add_styled_slide_base(prs, "CDC Ẩn danh Bệnh nhân & Khai phá Luật Kết hợp (Apriori / FP-Growth)")
    if "markov_apriori" in figs:
        s_ar.shapes.add_picture(str(figs["markov_apriori"]), PptIn(0.6), PptIn(1.50), width=PptIn(12.1))
    _add_white_card(
        s_ar, 0.6, 5.82, 12.13, 1.38,
        "Kết nối Luồng CDC Thời gian thực (1.508 bệnh nhân ẩn danh, 3.332 mũi tiêm) sang Data Mining",
        [
            "Debezium v2 + Kafka đồng bộ ẩn danh bảng patients sang dwh.dim_patient (loại bỏ full_name & citizen_hash, xử lý Placeholder).",
            "Apriori phát hiện quy luật rõ nét: [Tăng huyết áp + Mới tiêm 1 mũi] ⇒ Dương tính (Conf 73,8%, Lift 2,70x); [Chưa tiêm + ≥60 tuổi] ⇒ Dương tính (Conf 61,9%, Lift 2,26x).",
        ],
        accent_rgb=PptRGB(0x0F, 0x76, 0x6E),
    )

    # Di chuyển 4 slide mới (đang ở cuối index 17, 18, 19, 20) lên ngay sau Slide 13 (tức index 13, 14, 15, 16)
    for offset in range(4):
        _move_slide(prs, orig_count + offset, 13 + offset)

    prs.save(str(SLIDE_PPTX))
    print(f"[+] Đã cập nhật hoàn chỉnh Slide PowerPoint: {SLIDE_PPTX} ({len(prs.slides)} slides, {SLIDE_PPTX.stat().st_size:,} bytes)")


def main():
    with tempfile.TemporaryDirectory() as tmp:
        figs = prepare_figures(Path(tmp))
        update_baocao_docx(figs)
        update_slide_pptx(figs)


if __name__ == "__main__":
    main()
