#!/usr/bin/env python3
"""
HL跟单效果追踪系统 - 可视化图表生成 v3
修复：
  1. X轴时间显示：添加chartjs-adapter-date-fns CDN，x轴type改为'time'
  2. 空数据检查：避免chart_data[key]['te'][-1]报IndexError
  3. 所有图表统一使用time轴配置
  【P1修复#10】完善空数据保护：
    - 提取safe_last()辅助函数，统一安全访问
    - 增加数据完整性校验（ts/te列表长度一致）
    - 空数据图表输出友好提示而非空白
"""

import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path

from paths import BASE_DIR, DATA_DIR
DB_PATH = str(DATA_DIR / 'net_value_history.db')
CHART_DIR = str(DATA_DIR / 'charts')


def safe_last(lst, fmt='dollar', default='N/A'):
    """【P1修复#10】安全获取列表最后一个元素
    Args:
        lst: 数据列表
        fmt: 格式类型 'dollar'=美元格式, 'pct'=百分比, 'raw'=原值
        default: 空列表时的默认返回值
    """
    if not lst:
        return default
    val = lst[-1]
    if fmt == 'dollar':
        return f"${val:.2f}"
    elif fmt == 'pct':
        return f"{val:.2f}%"
    return val


def get_all_data(days=30):
    db = sqlite3.connect(DB_PATH)
    c = db.cursor()
    
    start_ts = int((datetime.now() - timedelta(days=days)).timestamp())
    
    c.execute('''
        SELECT ts, instance_id, role, account_value, total_equity, copy_ratio
        FROM net_value_snapshots
        WHERE ts > ?
        ORDER BY ts
    ''', (start_ts,))
    
    data = c.fetchall()
    
    # 查询资金流动
    c.execute('''
        SELECT ts, instance_id, role, flow_type, amount
        FROM fund_flows
        WHERE ts > ?
        ORDER BY ts
    ''', (start_ts,))
    
    flows = c.fetchall()
    
    db.close()
    
    return data, flows

def calculate_returns(data):
    """计算收益率"""
    returns = {}
    
    for key in ['lf_leader', 'lf_follower', 'hf_leader', 'hf_follower']:
        instance_id, role = key.split('_')
        returns[key] = []
        
        # 筛选该实例和角色的数据
        filtered = [(d[0], d[4]) for d in data if d[1] == instance_id and d[2] == role]
        
        if filtered:
            base_value = filtered[0][1]
            for ts, te in filtered:
                ret_pct = ((te - base_value) / base_value * 100) if base_value > 0 else 0
                returns[key].append({'x': ts * 1000, 'y': ret_pct})
    return returns

# ====== 修复：通用time轴配置（作为JS字符串模板） ======
TIME_AXIS_JS = """type: 'time',
        time: {
            unit: 'day',
            tooltipFormat: 'yyyy-MM-dd HH:mm',
            displayFormats: {
                day: 'MM-dd',
                hour: 'MM-dd HH:mm'
            }
        },
        title: {
            display: true,
            text: '\u65f6\u95f4'
        }"""

def generate_chart_html(days=30):
    data, flows = get_all_data(days)
    
    if not data:
        return "<html><body><h1>No data available</h1><p>\u6682\u65e0\u6570\u636e\uff0c\u8bf7\u786e\u8ba4\u6570\u636e\u91c7\u96c6\u662f\u5426\u6b63\u5e38\u8fd0\u884c</p></body></html>"
    
    # 按实例和角色分组
    chart_data = {
        'lf_leader': {'ts': [], 'te': []},
        'lf_follower': {'ts': [], 'te': [], 'ratio': []},
        'hf_leader': {'ts': [], 'te': []},
        'hf_follower': {'ts': [], 'te': [], 'ratio': []}
    }
    
    for row in data:
        ts, instance_id, role, av, te, ratio = row
        key = f"{instance_id}_{role}"
        
        if key in chart_data:
            chart_data[key]['ts'].append(ts * 1000)
            chart_data[key]['te'].append(te)
            if ratio is not None:
                chart_data[key]['ratio'].append({'x': ts * 1000, 'y': ratio * 100})
    
    # 【P1修复#10】数据完整性校验：确保ts和te列表长度一致
    for key in chart_data:
        ts_len = len(chart_data[key]['ts'])
        te_len = len(chart_data[key]['te'])
        if ts_len != te_len:
            min_len = min(ts_len, te_len)
            chart_data[key]['ts'] = chart_data[key]['ts'][:min_len]
            chart_data[key]['te'] = chart_data[key]['te'][:min_len]
            print(f"Warning: {key} ts/te length mismatch ({ts_len}/{te_len}), truncated to {min_len}")
    
    # 计算收益率
    returns = calculate_returns(data)
    
    # 资金进出统计
    flow_stats = {'lf_deposit': 0, 'lf_withdraw': 0, 'hf_deposit': 0, 'hf_withdraw': 0}
    for f in flows:
        ts, instance_id, role, flow_type, amount = f
        prefix = instance_id
        if flow_type in ['deposit', 'transfer_in']:
            flow_stats[f'{prefix}_deposit'] += amount
        else:
            flow_stats[f'{prefix}_withdraw'] += amount
    
    # 【P1修复#10】使用safe_last统一安全访问，避免[-1]索引报错
    lf_leader_last = safe_last(chart_data['lf_leader']['te'], 'dollar')
    lf_follower_last = safe_last(chart_data['lf_follower']['te'], 'dollar')
    hf_leader_last = safe_last(chart_data['hf_leader']['te'], 'dollar')
    hf_follower_last = safe_last(chart_data['hf_follower']['te'], 'dollar')
    
    # 准备JS数据（避免在f-string中使用嵌套大括号）
    lf_leader_data = json.dumps([{"x": t, "y": v} for t, v in zip(chart_data["lf_leader"]["ts"], chart_data["lf_leader"]["te"])])
    lf_follower_data = json.dumps([{"x": t, "y": v} for t, v in zip(chart_data["lf_follower"]["ts"], chart_data["lf_follower"]["te"])])
    hf_leader_data = json.dumps([{"x": t, "y": v} for t, v in zip(chart_data["hf_leader"]["ts"], chart_data["hf_leader"]["te"])])
    hf_follower_data = json.dumps([{"x": t, "y": v} for t, v in zip(chart_data["hf_follower"]["ts"], chart_data["hf_follower"]["te"])])
    returns_lf_leader = json.dumps(returns['lf_leader'])
    returns_lf_follower = json.dumps(returns['lf_follower'])
    returns_hf_leader = json.dumps(returns['hf_leader'])
    returns_hf_follower = json.dumps(returns['hf_follower'])
    ratio_lf = json.dumps(chart_data["lf_follower"]["ratio"])
    ratio_hf = json.dumps(chart_data["hf_follower"]["ratio"])
    
    gen_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # 使用format而非f-string，避免大括号转义地狱
    html = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>HL Copytrade Performance</title>
    <!-- 【修复】添加Chart.js date adapter CDN，支持time轴 -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns"></script>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .chart-container {{ background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ text-align: center; color: #333; }}
        h2 {{ color: #007bff; border-bottom: 2px solid #007bff; padding-bottom: 10px; }}
        .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
        .summary-card {{ background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .summary-card h3 {{ margin: 0 0 10px 0; color: #666; font-size: 14px; }}
        .summary-card .value {{ font-size: 24px; font-weight: bold; color: #333; }}
        .positive {{ color: #28a745; }}
        .negative {{ color: #dc3545; }}
        .no-data {{ color: #999; font-style: italic; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>\U0001f4ca HL\u8ddf\u5355\u6548\u679c\u8ffd\u8e2a\u7cfb\u7edf</h1>
        
        <p style="text-align: center; color: #666;">\u6700\u8fd1{days}\u5929\u6570\u636e | \u751f\u6210\u65f6\u95f4: {gen_time}</p>
        
        <div class="summary">
            <div class="summary-card">
                <h3>\u4f4e\u9891\u4e95\u5927\u51c0\u503c</h3>
                <div class="value">{lf_leader_last}</div>
            </div>
            <div class="summary-card">
                <h3>\u4f4e\u9891\u7528\u6237\u51c0\u503c</h3>
                <div class="value">{lf_follower_last}</div>
            </div>
            <div class="summary-card">
                <h3>\u9ad8\u9891\u4e95\u5927\u51c0\u503c</h3>
                <div class="value">{hf_leader_last}</div>
            </div>
            <div class="summary-card">
                <h3>\u9ad8\u9891\u7528\u6237\u51c0\u503c</h3>
                <div class="value">{hf_follower_last}</div>
            </div>
        </div>
        
        <div class="chart-container">
            <h2>1. \u4f4e\u9891\u8d26\u6237 - \u51c0\u503c\u53d8\u5316\u8d8b\u52bf</h2>
            <canvas id="lf_netvalue"></canvas>
        </div>
        
        <div class="chart-container">
            <h2>2. \u9ad8\u9891\u8d26\u6237 - \u51c0\u503c\u53d8\u5316\u8d8b\u52bf</h2>
            <canvas id="hf_netvalue"></canvas>
        </div>
        
        <div class="chart-container">
            <h2>3. \u6536\u76ca\u7387\u5bf9\u6bd4</h2>
            <canvas id="returns_comparison"></canvas>
        </div>
        
        <div class="chart-container">
            <h2>4. \u8ddf\u5355\u6bd4\u4f8b\u53d8\u5316</h2>
            <canvas id="copy_ratio"></canvas>
        </div>
        
        <div class="chart-container">
            <h2>5. \u8d44\u91d1\u8fdb\u51fa\u7edf\u8ba1</h2>
            <canvas id="fund_flows"></canvas>
        </div>
    </div>
    
    <script>
        // 【修复】使用time轴替代linear轴，配合chartjs-adapter-date-fns
        
        // 1. \u4f4e\u9891\u8d26\u6237\u51c0\u503c\u56fe
        new Chart(document.getElementById('lf_netvalue'), {{
            type: 'line',
            data: {{
                datasets: [
                    {{
                        label: '\u4e95\u5927\uff08Leader\uff09',
                        data: {lf_leader_data},
                        borderColor: 'rgb(54, 162, 235)',
                        backgroundColor: 'rgba(54, 162, 235, 0.1)',
                        tension: 0.1
                    }},
                    {{
                        label: '\u7528\u6237\uff08Follower\uff09',
                        data: {lf_follower_data},
                        borderColor: 'rgb(255, 99, 132)',
                        backgroundColor: 'rgba(255, 99, 132, 0.1)',
                        tension: 0.1
                    }}
                ]
            }},
            options: {{
                responsive: true,
                scales: {{
                    x: {{{time_axis_js}}},
                    y: {{ title: {{ display: true, text: 'Total Equity ($)' }} }}
                }}
            }}
        }});
        
        // 2. \u9ad8\u9891\u8d26\u6237\u51c0\u503c\u56fe
        new Chart(document.getElementById('hf_netvalue'), {{
            type: 'line',
            data: {{
                datasets: [
                    {{
                        label: '\u4e95\u5927\uff08Leader\uff09',
                        data: {hf_leader_data},
                        borderColor: 'rgb(75, 192, 192)',
                        backgroundColor: 'rgba(75, 192, 192, 0.1)',
                        tension: 0.1
                    }},
                    {{
                        label: '\u7528\u6237\uff08Follower\uff09',
                        data: {hf_follower_data},
                        borderColor: 'rgb(255, 205, 86)',
                        backgroundColor: 'rgba(255, 205, 86, 0.1)',
                        tension: 0.1
                    }}
                ]
            }},
            options: {{
                responsive: true,
                scales: {{
                    x: {{{time_axis_js}}},
                    y: {{ title: {{ display: true, text: 'Total Equity ($)' }} }}
                }}
            }}
        }});
        
        // 3. \u6536\u76ca\u7387\u5bf9\u6bd4\u56fe
        new Chart(document.getElementById('returns_comparison'), {{
            type: 'line',
            data: {{
                datasets: [
                    {{
                        label: '\u4f4e\u9891\u4e95\u5927\u6536\u76ca\u7387',
                        data: {returns_lf_leader},
                        borderColor: 'rgb(54, 162, 235)',
                        tension: 0.1
                    }},
                    {{
                        label: '\u4f4e\u9891\u7528\u6237\u6536\u76ca\u7387',
                        data: {returns_lf_follower},
                        borderColor: 'rgb(255, 99, 132)',
                        tension: 0.1
                    }},
                    {{
                        label: '\u9ad8\u9891\u4e95\u5927\u6536\u76ca\u7387',
                        data: {returns_hf_leader},
                        borderColor: 'rgb(75, 192, 192)',
                        tension: 0.1
                    }},
                    {{
                        label: '\u9ad8\u9891\u7528\u6237\u6536\u76ca\u7387',
                        data: {returns_hf_follower},
                        borderColor: 'rgb(255, 205, 86)',
                        tension: 0.1
                    }}
                ]
            }},
            options: {{
                responsive: true,
                scales: {{
                    x: {{{time_axis_js}}},
                    y: {{ title: {{ display: true, text: '\u6536\u76ca\u7387 (%)' }} }}
                }}
            }}
        }});
        
        // 4. \u8ddf\u5355\u6bd4\u4f8b\u56fe
        new Chart(document.getElementById('copy_ratio'), {{
            type: 'line',
            data: {{
                datasets: [
                    {{
                        label: '\u4f4e\u9891\u8ddf\u5355\u6bd4\u4f8b',
                        data: {ratio_lf},
                        borderColor: 'rgb(255, 99, 132)',
                        backgroundColor: 'rgba(255, 99, 132, 0.1)',
                        tension: 0.1
                    }},
                    {{
                        label: '\u9ad8\u9891\u8ddf\u5355\u6bd4\u4f8b',
                        data: {ratio_hf},
                        borderColor: 'rgb(255, 205, 86)',
                        backgroundColor: 'rgba(255, 205, 86, 0.1)',
                        tension: 0.1
                    }}
                ]
            }},
            options: {{
                responsive: true,
                scales: {{
                    x: {{{time_axis_js}}},
                    y: {{ title: {{ display: true, text: '\u8ddf\u5355\u6bd4\u4f8b (%)' }} }}
                }}
            }}
        }});
        
        // 5. \u8d44\u91d1\u8fdb\u51fa\u7edf\u8ba1\u56fe
        new Chart(document.getElementById('fund_flows'), {{
            type: 'bar',
            data: {{
                labels: ['\u4f4e\u9891\u5145\u503c', '\u4f4e\u9891\u63d0\u73b0', '\u9ad8\u9891\u5145\u503c', '\u9ad8\u9891\u63d0\u73b0'],
                datasets: [{{
                    label: '\u8d44\u91d1\u8fdb\u51fa ($)',
                    data: [{lf_dep}, {lf_wd}, {hf_dep}, {hf_wd}],
                    backgroundColor: [
                        'rgba(40, 167, 69, 0.7)',
                        'rgba(220, 53, 69, 0.7)',
                        'rgba(40, 167, 69, 0.7)',
                        'rgba(220, 53, 69, 0.7)'
                    ],
                    borderColor: [
                        'rgb(40, 167, 69)',
                        'rgb(220, 53, 69)',
                        'rgb(40, 167, 69)',
                        'rgb(220, 53, 69)'
                    ],
                    borderWidth: 1
                }}]
            }},
            options: {{
                responsive: true,
                scales: {{
                    y: {{ beginAtZero: true, title: {{ display: true, text: '\u91d1\u989d ($)' }} }}
                }}
            }}
        }});
    </script>
</body>
</html>'''.format(
        days=days,
        gen_time=gen_time,
        lf_leader_last=lf_leader_last,
        lf_follower_last=lf_follower_last,
        hf_leader_last=hf_leader_last,
        hf_follower_last=hf_follower_last,
        lf_leader_data=lf_leader_data,
        lf_follower_data=lf_follower_data,
        hf_leader_data=hf_leader_data,
        hf_follower_data=hf_follower_data,
        returns_lf_leader=returns_lf_leader,
        returns_lf_follower=returns_lf_follower,
        returns_hf_leader=returns_hf_leader,
        returns_hf_follower=returns_hf_follower,
        ratio_lf=ratio_lf,
        ratio_hf=ratio_hf,
        time_axis_js=TIME_AXIS_JS,
        lf_dep=f"{flow_stats['lf_deposit']:.2f}",
        lf_wd=f"{flow_stats['lf_withdraw']:.2f}",
        hf_dep=f"{flow_stats['hf_deposit']:.2f}",
        hf_wd=f"{flow_stats['hf_withdraw']:.2f}",
    )
    
    return html

if __name__ == '__main__':
    import sys
    
    days = 30
    if len(sys.argv) > 1:
        days = int(sys.argv[1])
    
    Path(CHART_DIR).mkdir(parents=True, exist_ok=True)
    
    html = generate_chart_html(days)
    output_path = f'{CHART_DIR}/charts_{datetime.now().strftime("%Y%m%d")}.html'
    
    with open(output_path, 'w') as f:
        f.write(html)
    
    print(f"Charts generated: {output_path}")
