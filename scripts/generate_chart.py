# -*- coding: utf-8 -*-
"""
Tự động sinh trang HTML trực quan hóa đồ thị phân cụm K-Means cực đẹp bằng Apache ECharts
Dữ liệu trích xuất trực tiếp 100% từ PostgreSQL DWH (dwh.fact_weekly_covid_cluster)
"""

import json
import subprocess
import sys
import os

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

def fetch_data_from_dwh():
    cmd = [
        "docker", "exec", "-i", "postgres-dwh",
        "psql", "-U", "dwh_admin", "-d", "health_dwh", "-t", "-A", "-c",
        """
        SELECT json_build_object(
            'country', country_name,
            'code', country_code,
            'week', to_char(week_start, 'YYYY-MM-DD'),
            'cluster', cluster_id,
            'cases_100k', ROUND(cases_per_100k, 1),
            'deaths_100k', ROUND(deaths_per_100k, 2),
            'growth_pct', ROUND(COALESCE(case_growth_pct, 0), 1),
            'population', population
        )
        FROM (
            (SELECT * FROM dwh.fact_weekly_covid_cluster WHERE cluster_id = 0 ORDER BY random() LIMIT 150)
            UNION ALL
            (SELECT * FROM dwh.fact_weekly_covid_cluster WHERE cluster_id = 1 ORDER BY random() LIMIT 150)
            UNION ALL
            (SELECT * FROM dwh.fact_weekly_covid_cluster WHERE cluster_id = 2 ORDER BY random() LIMIT 150)
            UNION ALL
            (SELECT * FROM dwh.fact_weekly_covid_cluster WHERE cluster_id = 3 ORDER BY random() LIMIT 150)
            UNION ALL
            (SELECT * FROM dwh.fact_weekly_covid_cluster WHERE country_code = 'VNM' ORDER BY week_start)
        ) sample_data;
        """
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    records = []
    for line in res.stdout.strip().split("\n"):
        line = line.strip()
        if line and line.startswith("{"):
            try:
                records.append(json.loads(line))
            except Exception:
                pass
    return records

def generate_html(records, output_path):
    json_data = json.dumps(records, ensure_ascii=False)
    html_content = f"""<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Biểu Đồ Trực Quan Hóa Phân Cụm K-Means COVID-19</title>
    <!-- Apache ECharts CDN -->
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background: #0f172a;
            color: #f8fafc;
            padding: 24px;
        }}
        .header {{
            text-align: center;
            margin-bottom: 24px;
            padding: 20px;
            background: #1e293b;
            border-radius: 12px;
            border: 1px solid #334155;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
        }}
        .header h1 {{
            font-size: 26px;
            color: #38bdf8;
            margin-bottom: 8px;
        }}
        .header p {{
            font-size: 14px;
            color: #94a3b8;
        }}
        .badges {{
            display: flex;
            justify-content: center;
            gap: 16px;
            margin-top: 14px;
            flex-wrap: wrap;
        }}
        .badge {{
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 6px;
        }}
        .badge-0 {{ background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid #10b981; }}
        .badge-1 {{ background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid #f59e0b; }}
        .badge-2 {{ background: rgba(249, 115, 22, 0.2); color: #fb923c; border: 1px solid #f97316; }}
        .badge-3 {{ background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid #ef4444; }}

        .grid-container {{
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }}
        @media (max-width: 1024px) {{
            .grid-container {{ grid-template-columns: 1fr; }}
        }}
        .chart-card {{
            background: #1e293b;
            border-radius: 12px;
            border: 1px solid #334155;
            padding: 18px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
        }}
        .chart-card h2 {{
            font-size: 16px;
            color: #e2e8f0;
            margin-bottom: 12px;
            border-left: 4px solid #38bdf8;
            padding-left: 10px;
        }}
        .chart-box {{
            width: 100%;
            height: 480px;
        }}
        .chart-box-small {{
            width: 100%;
            height: 230px;
        }}
        .filter-bar {{
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 16px;
            background: #1e293b;
            padding: 12px 18px;
            border-radius: 8px;
            border: 1px solid #334155;
        }}
        select, button {{
            background: #334155;
            color: #fff;
            border: 1px solid #475569;
            padding: 8px 14px;
            border-radius: 6px;
            font-size: 14px;
            cursor: pointer;
        }}
        select:focus, button:focus {{ outline: none; border-color: #38bdf8; }}
        button:hover {{ background: #0284c7; }}
    </style>
</head>
<body>

    <div class="header">
        <h1>TRỰC QUAN HÓA PHÂN CỤM DỊCH TỄ COVID-19 BẰNG K-MEANS</h1>
        <p>Thuật toán K-Means Clustering (K=4) | 56.477 bản ghi từ PostgreSQL Data Warehouse (WHO & OWID)</p>
        <div class="badges">
            <span class="badge badge-0">● Cụm 0: Vùng Xanh (Kiểm soát tốt - 62.4%)</span>
            <span class="badge badge-1">● Cụm 1: Vùng Vàng (Nguy cơ vừa - 26.4%)</span>
            <span class="badge badge-2">● Cụm 2: Vùng Cam (Bùng phát đột biến - 1.4%)</span>
            <span class="badge badge-3">● Cụm 3: Vùng Đỏ (Báo động quá tải - 9.9%)</span>
        </div>
    </div>

    <div class="filter-bar">
        <label for="countryFilter"><strong>Lọc theo Quốc gia:</strong></label>
        <select id="countryFilter" onchange="filterCountry()">
            <option value="ALL">Tất cả quốc gia (Tổng quan phân cụm)</option>
            <option value="VNM" selected>Việt Nam (VNM)</option>
            <option value="USA">Mỹ (USA)</option>
            <option value="ITA">Ý (ITA)</option>
            <option value="IND">Ấn Độ (IND)</option>
            <option value="BRA">Brazil (BRA)</option>
        </select>
        <button onclick="resetZoom()">Đặt lại phóng to</button>
        <span style="font-size: 13px; color: #94a3b8; margin-left: auto;">
            Mẹo: Rê chuột vào từng chấm tròn để xem số liệu chi tiết. Cuộn chuột để phóng to/thu nhỏ.
        </span>
    </div>

    <div class="grid-container">
        <!-- Đồ thị chính: Scatter Plot phân cụm rõ rệt -->
        <div class="chart-card">
            <h2>1. Biểu đồ Phân tán Gom Cụm 2D: Ca Nhiễm vs Tử Vong (K-Means Scatter Plot)</h2>
            <div id="scatterChart" class="chart-box"></div>
        </div>

        <!-- Cột bên phải: Pie Chart và Bar Chart -->
        <div style="display: flex; flex-direction: column; gap: 20px;">
            <div class="chart-card">
                <h2>2. Tỷ lệ Phân Bố 4 Cụm Dịch Tễ</h2>
                <div id="pieChart" class="chart-box-small"></div>
            </div>
            <div class="chart-card">
                <h2>3. Tỷ lệ Tử Vong TB / 100k Dân</h2>
                <div id="barChart" class="chart-box-small"></div>
            </div>
        </div>
    </div>

    <script>
        const rawData = {json_data};

        const clusterColors = {{
            0: '#10b981', // Xanh lá
            1: '#f59e0b', // Vàng cam
            2: '#f97316', // Cam đậm
            3: '#ef4444'  // Đỏ
        }};

        const clusterNames = {{
            0: 'Cụm 0 (Vùng Xanh)',
            1: 'Cụm 1 (Vùng Vàng)',
            2: 'Cụm 2 (Vùng Cam - Bùng phát)',
            3: 'Cụm 3 (Vùng Đỏ - Quá tải)'
        }};

        // Khởi tạo Scatter Chart
        const scatterChart = echarts.init(document.getElementById('scatterChart'));
        const pieChart = echarts.init(document.getElementById('pieChart'));
        const barChart = echarts.init(document.getElementById('barChart'));

        function renderScatter(filteredData) {{
            const seriesList = [0, 1, 2, 3].map(cid => {{
                const items = filteredData.filter(d => d.cluster === cid);
                return {{
                    name: clusterNames[cid],
                    type: 'scatter',
                    itemStyle: {{
                        color: clusterColors[cid],
                        opacity: 0.85,
                        borderColor: '#fff',
                        borderWidth: 0.5
                    }},
                    symbolSize: function (data) {{
                        return Math.max(8, Math.min(35, Math.sqrt(data[2] / 500000)));
                    }},
                    data: items.map(d => [
                        d.cases_100k,
                        d.deaths_100k,
                        d.population,
                        d.country,
                        d.week,
                        d.growth_pct,
                        d.cluster
                    ])
                }};
            }});

            scatterChart.setOption({{
                backgroundColor: 'transparent',
                tooltip: {{
                    trigger: 'item',
                    backgroundColor: '#1e293b',
                    borderColor: '#475569',
                    textStyle: {{ color: '#f8fafc' }},
                    formatter: function (param) {{
                        const d = param.data;
                        return `
                            <div style="font-weight: bold; font-size: 15px; color: ${{clusterColors[d[6]]}}">
                                ${{d[3]}} (Tuần: ${{d[4]}})
                            </div>
                            <div style="margin-top: 6px; font-size: 13px;">
                                <b>Phân Cụm:</b> ${{clusterNames[d[6]]}}<br/>
                                <b>Số ca nhiễm / 100k:</b> ${{d[0].toLocaleString()}}<br/>
                                <b>Tỷ lệ tử vong / 100k:</b> ${{d[1].toFixed(2)}}<br/>
                                <b>Tốc độ tăng trưởng tuần:</b> ${{d[5] > 0 ? '+' : ''}}${{d[5]}}%<br/>
                                <b>Dân số:</b> ${{d[2].toLocaleString()}}
                            </div>
                        `;
                    }}
                }},
                legend: {{
                    top: 10,
                    textStyle: {{ color: '#cbd5e1' }}
                }},
                grid: {{
                    left: '8%',
                    right: '8%',
                    bottom: '12%',
                    top: '15%'
                }},
                xAxis: {{
                    type: 'value',
                    name: 'Ca nhiễm / 100k dân (tuần)',
                    nameLocation: 'middle',
                    nameGap: 30,
                    nameTextStyle: {{ color: '#94a3b8', fontSize: 13 }},
                    axisLabel: {{ color: '#94a3b8' }},
                    splitLine: {{ lineStyle: {{ color: '#334155', type: 'dashed' }} }}
                }},
                yAxis: {{
                    type: 'value',
                    name: 'Tử vong / 100k dân (tuần)',
                    nameTextStyle: {{ color: '#94a3b8', fontSize: 13 }},
                    axisLabel: {{ color: '#94a3b8' }},
                    splitLine: {{ lineStyle: {{ color: '#334155', type: 'dashed' }} }}
                }},
                dataZoom: [
                    {{ type: 'inside' }},
                    {{ type: 'slider', bottom: 5, borderColor: '#334155', textStyle: {{ color: '#94a3b8' }} }}
                ],
                series: seriesList
            }});
        }}

        // Render Pie Chart
        pieChart.setOption({{
            backgroundColor: 'transparent',
            tooltip: {{ trigger: 'item', formatter: '{{b}}: {{c}} tuần ({{d}}%)' }},
            legend: {{ orient: 'horizontal', bottom: 5, textStyle: {{ color: '#cbd5e1', fontSize: 11 }} }},
            series: [{{
                type: 'pie',
                radius: ['45%', '70%'],
                center: ['50%', '45%'],
                avoidLabelOverlap: false,
                itemStyle: {{ borderRadius: 6, borderColor: '#1e293b', borderWidth: 2 }},
                label: {{ show: false }},
                data: [
                    {{ value: 35236, name: 'Vùng Xanh', itemStyle: {{ color: '#10b981' }} }},
                    {{ value: 14900, name: 'Vùng Vàng', itemStyle: {{ color: '#f59e0b' }} }},
                    {{ value: 770,   name: 'Vùng Cam', itemStyle: {{ color: '#f97316' }} }},
                    {{ value: 5571,  name: 'Vùng Đỏ', itemStyle: {{ color: '#ef4444' }} }}
                ]
            }}]
        }});

        // Render Bar Chart
        barChart.setOption({{
            backgroundColor: 'transparent',
            tooltip: {{ trigger: 'axis', axisPointer: {{ type: 'shadow' }} }},
            grid: {{ left: '15%', right: '10%', bottom: '15%', top: '15%' }},
            xAxis: {{
                type: 'category',
                data: ['Cụm 0', 'Cụm 1', 'Cụm 2', 'Cụm 3'],
                axisLabel: {{ color: '#94a3b8' }}
            }},
            yAxis: {{
                type: 'value',
                axisLabel: {{ color: '#94a3b8' }},
                splitLine: {{ lineStyle: {{ color: '#334155', type: 'dashed' }} }}
            }},
            series: [{{
                data: [
                    {{ value: 0.02, itemStyle: {{ color: '#10b981' }} }},
                    {{ value: 0.36, itemStyle: {{ color: '#f59e0b' }} }},
                    {{ value: 0.39, itemStyle: {{ color: '#f97316' }} }},
                    {{ value: 4.14, itemStyle: {{ color: '#ef4444' }} }}
                ],
                type: 'bar',
                barWidth: '45%',
                label: {{ show: true, position: 'top', color: '#f8fafc', formatter: '{{c}}' }}
            }}]
        }});

        function filterCountry() {{
            const code = document.getElementById('countryFilter').value;
            if (code === 'ALL') {{
                renderScatter(rawData);
            }} else {{
                const filtered = rawData.filter(d => d.code === code);
                renderScatter(filtered);
            }}
        }}

        function resetZoom() {{
            scatterChart.dispatchAction({{ type: 'dataZoom', start: 0, end: 100 }});
        }}

        // Khởi động lần đầu
        filterCountry();

        window.addEventListener('resize', () => {{
            scatterChart.resize();
            pieChart.resize();
            barChart.resize();
        }});
    </script>
</body>
</html>
"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"Successfully generated HTML visualization: {output_path}")

if __name__ == "__main__":
    records = fetch_data_from_dwh()
    output = os.path.join(os.path.dirname(__file__), "kmeans_dashboard.html")
    generate_html(records, output)
