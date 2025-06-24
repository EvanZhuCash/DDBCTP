"""
故障排查系统 - 自动监控和恢复机制
实现系统健康检查、错误监控、自动故障恢复
"""

import time
import threading
import queue
import psutil
import json
import smtplib
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from email.mime.text import MIMEText
import dolphindb as ddb


class HealthCheckResult:
    """健康检查结果"""
    
    def __init__(self, component: str, status: str, message: str = "", details: Dict = None):
        self.component = component
        self.status = status  # HEALTHY, WARNING, CRITICAL, UNKNOWN
        self.message = message
        self.details = details or {}
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict:
        return {
            'component': self.component,
            'status': self.status,
            'message': self.message,
            'details': self.details,
            'timestamp': self.timestamp.isoformat()
        }


class AlertManager:
    """警报管理器"""
    
    def __init__(self):
        self.alert_rules = {}
        self.notification_channels = []
        self.alert_history = []
        self.suppression_rules = {}  # 警报抑制规则
    
    def add_alert_rule(self, rule_name: str, condition: Callable, severity: str, message: str):
        """添加警报规则"""
        self.alert_rules[rule_name] = {
            'condition': condition,
            'severity': severity,
            'message': message,
            'enabled': True,
            'last_triggered': None
        }
    
    def add_notification_channel(self, channel_type: str, config: Dict):
        """添加通知渠道"""
        self.notification_channels.append({
            'type': channel_type,
            'config': config,
            'enabled': True
        })
    
    def check_alerts(self, health_results: List[HealthCheckResult]):
        """检查警报条件"""
        triggered_alerts = []
        
        for rule_name, rule in self.alert_rules.items():
            if not rule['enabled']:
                continue
            
            try:
                if rule['condition'](health_results):
                    # 检查抑制规则
                    if not self._is_suppressed(rule_name):
                        alert = {
                            'rule_name': rule_name,
                            'severity': rule['severity'],
                            'message': rule['message'],
                            'timestamp': datetime.now(),
                            'health_results': [r.to_dict() for r in health_results]
                        }
                        
                        triggered_alerts.append(alert)
                        self.alert_history.append(alert)
                        rule['last_triggered'] = datetime.now()
                        
                        # 发送通知
                        self._send_notifications(alert)
            
            except Exception as e:
                print(f"警报规则 {rule_name} 检查失败: {e}")
        
        return triggered_alerts
    
    def _is_suppressed(self, rule_name: str) -> bool:
        """检查警报是否被抑制"""
        if rule_name in self.suppression_rules:
            suppression = self.suppression_rules[rule_name]
            if datetime.now() < suppression['until']:
                return True
        return False
    
    def _send_notifications(self, alert: Dict):
        """发送通知"""
        for channel in self.notification_channels:
            if not channel['enabled']:
                continue
            
            try:
                if channel['type'] == 'email':
                    self._send_email_notification(alert, channel['config'])
                elif channel['type'] == 'webhook':
                    self._send_webhook_notification(alert, channel['config'])
                elif channel['type'] == 'console':
                    self._send_console_notification(alert)
            
            except Exception as e:
                print(f"发送 {channel['type']} 通知失败: {e}")
    
    def _send_email_notification(self, alert: Dict, config: Dict):
        """发送邮件通知"""
        subject = f"[{alert['severity']}] 系统警报: {alert['rule_name']}"
        body = f"""
        警报时间: {alert['timestamp']}
        警报级别: {alert['severity']}
        警报规则: {alert['rule_name']}
        警报信息: {alert['message']}
        
        详细信息:
        {json.dumps(alert['health_results'], indent=2, ensure_ascii=False)}
        """
        
        msg = MIMEText(body, 'plain', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = config['from_email']
        msg['To'] = config['to_email']
        
        with smtplib.SMTP(config['smtp_server'], config['smtp_port']) as server:
            if config.get('use_tls'):
                server.starttls()
            if config.get('username'):
                server.login(config['username'], config['password'])
            server.send_message(msg)
    
    def _send_webhook_notification(self, alert: Dict, config: Dict):
        """发送Webhook通知"""
        payload = {
            'alert_type': alert['rule_name'],
            'severity': alert['severity'],
            'message': alert['message'],
            'timestamp': alert['timestamp'].isoformat(),
            'details': alert['health_results']
        }
        
        response = requests.post(
            config['url'],
            json=payload,
            headers=config.get('headers', {}),
            timeout=config.get('timeout', 10)
        )
        response.raise_for_status()
    
    def _send_console_notification(self, alert: Dict):
        """发送控制台通知"""
        print(f"\n🚨 [{alert['severity']}] {alert['rule_name']}")
        print(f"时间: {alert['timestamp']}")
        print(f"信息: {alert['message']}")


class SystemHealthMonitor:
    """系统健康监控器"""
    
    def __init__(self):
        self.session = ddb.session()
        self.session.connect("localhost", 8848, "admin", "123456")
        self.monitoring = False
        self.monitor_thread = None
        self.alert_manager = AlertManager()
        self.recovery_manager = RecoveryManager(self.session)
        self.setup_monitoring_tables()
        self.setup_default_alerts()
    
    def setup_monitoring_tables(self):
        """设置监控表"""
        self.session.run("""
            // 系统健康状态表
            if(existsTable("system_health_status")){
                dropTable("system_health_status")
            }
            share streamTable(1000:0, `timestamp`component`status`message`cpu_percent`memory_percent`disk_percent, 
                [TIMESTAMP, SYMBOL, SYMBOL, STRING, DOUBLE, DOUBLE, DOUBLE]) as system_health_status
            
            // 错误日志表
            if(existsTable("error_logs")){
                dropTable("error_logs")
            }
            share streamTable(10000:0, `timestamp`level`component`error_type`message`stack_trace, 
                [TIMESTAMP, SYMBOL, SYMBOL, SYMBOL, STRING, STRING]) as error_logs
            
            // 性能指标表
            if(existsTable("performance_metrics")){
                dropTable("performance_metrics")
            }
            share streamTable(1000:0, `timestamp`metric_name`metric_value`unit, 
                [TIMESTAMP, SYMBOL, DOUBLE, SYMBOL]) as performance_metrics
        """)
    
    def setup_default_alerts(self):
        """设置默认警报规则"""
        # 高内存使用率警报
        self.alert_manager.add_alert_rule(
            "high_memory_usage",
            lambda results: any(r.component == "memory" and r.status == "CRITICAL" for r in results),
            "CRITICAL",
            "系统内存使用率过高"
        )
        
        # DolphinDB连接失败警报
        self.alert_manager.add_alert_rule(
            "ddb_connection_failed",
            lambda results: any(r.component == "dolphindb" and r.status == "CRITICAL" for r in results),
            "CRITICAL",
            "DolphinDB连接失败"
        )
        
        # 高延迟警报
        self.alert_manager.add_alert_rule(
            "high_latency",
            lambda results: any(r.component == "latency" and r.status == "WARNING" for r in results),
            "WARNING",
            "系统延迟过高"
        )
        
        # 添加控制台通知渠道
        self.alert_manager.add_notification_channel("console", {})
    
    def start_monitoring(self, interval: float = 30.0):
        """开始监控"""
        if self.monitoring:
            print("监控已在运行中")
            return
        
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, args=(interval,))
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        print(f"系统健康监控已启动，检查间隔: {interval}秒")
    
    def stop_monitoring(self):
        """停止监控"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        print("系统健康监控已停止")
    
    def _monitoring_loop(self, interval: float):
        """监控循环"""
        while self.monitoring:
            try:
                # 执行健康检查
                health_results = self.perform_health_checks()
                
                # 记录健康状态
                self._record_health_status(health_results)
                
                # 检查警报
                alerts = self.alert_manager.check_alerts(health_results)
                
                # 自动恢复
                if alerts:
                    self.recovery_manager.attempt_recovery(health_results, alerts)
                
            except Exception as e:
                print(f"监控循环错误: {e}")
            
            time.sleep(interval)
    
    def perform_health_checks(self) -> List[HealthCheckResult]:
        """执行健康检查"""
        results = []
        
        # 1. 系统资源检查
        results.extend(self._check_system_resources())
        
        # 2. DolphinDB连接检查
        results.append(self._check_dolphindb_connection())
        
        # 3. 流表状态检查
        results.extend(self._check_stream_tables())
        
        # 4. 引擎状态检查
        results.extend(self._check_stream_engines())
        
        # 5. 性能指标检查
        results.extend(self._check_performance_metrics())
        
        return results
    
    def _check_system_resources(self) -> List[HealthCheckResult]:
        """检查系统资源"""
        results = []
        
        # CPU检查
        cpu_percent = psutil.cpu_percent(interval=1)
        if cpu_percent > 90:
            status = "CRITICAL"
            message = f"CPU使用率过高: {cpu_percent:.1f}%"
        elif cpu_percent > 70:
            status = "WARNING"
            message = f"CPU使用率较高: {cpu_percent:.1f}%"
        else:
            status = "HEALTHY"
            message = f"CPU使用率正常: {cpu_percent:.1f}%"
        
        results.append(HealthCheckResult("cpu", status, message, {"cpu_percent": cpu_percent}))
        
        # 内存检查
        memory = psutil.virtual_memory()
        if memory.percent > 90:
            status = "CRITICAL"
            message = f"内存使用率过高: {memory.percent:.1f}%"
        elif memory.percent > 80:
            status = "WARNING"
            message = f"内存使用率较高: {memory.percent:.1f}%"
        else:
            status = "HEALTHY"
            message = f"内存使用率正常: {memory.percent:.1f}%"
        
        results.append(HealthCheckResult("memory", status, message, {
            "memory_percent": memory.percent,
            "total_gb": memory.total / (1024**3),
            "available_gb": memory.available / (1024**3)
        }))
        
        # 磁盘检查
        disk = psutil.disk_usage('/')
        disk_percent = (disk.used / disk.total) * 100
        if disk_percent > 90:
            status = "CRITICAL"
            message = f"磁盘使用率过高: {disk_percent:.1f}%"
        elif disk_percent > 80:
            status = "WARNING"
            message = f"磁盘使用率较高: {disk_percent:.1f}%"
        else:
            status = "HEALTHY"
            message = f"磁盘使用率正常: {disk_percent:.1f}%"
        
        results.append(HealthCheckResult("disk", status, message, {"disk_percent": disk_percent}))
        
        return results
    
    def _check_dolphindb_connection(self) -> HealthCheckResult:
        """检查DolphinDB连接"""
        try:
            # 测试简单查询
            result = self.session.run("1+1")
            if result == 2:
                return HealthCheckResult("dolphindb", "HEALTHY", "DolphinDB连接正常")
            else:
                return HealthCheckResult("dolphindb", "WARNING", "DolphinDB响应异常")
        
        except Exception as e:
            return HealthCheckResult("dolphindb", "CRITICAL", f"DolphinDB连接失败: {str(e)}")
    
    def _check_stream_tables(self) -> List[HealthCheckResult]:
        """检查流表状态"""
        results = []
        
        try:
            # 检查关键流表
            table_names = ["tickStream", "factor_stream", "orderStream"]
            
            for table_name in table_names:
                try:
                    table_info = self.session.run(f"""
                        if(existsTable("{table_name}")){{
                            select count(*) as row_count, 
                                   (now() - max(timestamp)) as last_update_ms
                            from {table_name}
                        }} else {{
                            table(0 as row_count, 999999 as last_update_ms)
                        }}
                    """)
                    
                    if len(table_info) > 0:
                        row_count = int(table_info.iloc[0]['row_count'])
                        last_update_ms = float(table_info.iloc[0]['last_update_ms'])
                        
                        if last_update_ms > 300000:  # 5分钟没有更新
                            status = "WARNING"
                            message = f"流表 {table_name} 长时间未更新"
                        else:
                            status = "HEALTHY"
                            message = f"流表 {table_name} 状态正常"
                        
                        results.append(HealthCheckResult(f"table_{table_name}", status, message, {
                            "row_count": row_count,
                            "last_update_ms": last_update_ms
                        }))
                    else:
                        results.append(HealthCheckResult(f"table_{table_name}", "CRITICAL", f"流表 {table_name} 不存在"))
                
                except Exception as e:
                    results.append(HealthCheckResult(f"table_{table_name}", "CRITICAL", f"检查流表 {table_name} 失败: {str(e)}"))
        
        except Exception as e:
            results.append(HealthCheckResult("stream_tables", "CRITICAL", f"流表检查失败: {str(e)}"))
        
        return results
    
    def _check_stream_engines(self) -> List[HealthCheckResult]:
        """检查流引擎状态"""
        results = []
        
        try:
            # 获取流引擎状态
            engine_status = self.session.run("getStreamEngineStatus()")
            
            if len(engine_status) == 0:
                results.append(HealthCheckResult("stream_engines", "WARNING", "没有运行中的流引擎"))
            else:
                for _, engine in engine_status.iterrows():
                    engine_name = engine.get('name', 'unknown')
                    status = engine.get('status', 'unknown')
                    
                    if status == 'running':
                        results.append(HealthCheckResult(f"engine_{engine_name}", "HEALTHY", f"引擎 {engine_name} 运行正常"))
                    else:
                        results.append(HealthCheckResult(f"engine_{engine_name}", "CRITICAL", f"引擎 {engine_name} 状态异常: {status}"))
        
        except Exception as e:
            results.append(HealthCheckResult("stream_engines", "CRITICAL", f"流引擎检查失败: {str(e)}"))
        
        return results
    
    def _check_performance_metrics(self) -> List[HealthCheckResult]:
        """检查性能指标"""
        results = []
        
        try:
            # 检查最近的延迟指标
            latency_data = self.session.run("""
                if(existsTable("latency_monitor")){
                    select avg(latency_us) as avg_latency, max(latency_us) as max_latency
                    from latency_monitor 
                    where timestamp > (now() - 300000)  // 最近5分钟
                } else {
                    table(0.0 as avg_latency, 0.0 as max_latency)
                }
            """)
            
            if len(latency_data) > 0:
                avg_latency = float(latency_data.iloc[0]['avg_latency'])
                max_latency = float(latency_data.iloc[0]['max_latency'])
                
                if avg_latency > 10000:  # 平均延迟超过10ms
                    status = "WARNING"
                    message = f"平均延迟过高: {avg_latency:.2f}μs"
                elif max_latency > 50000:  # 最大延迟超过50ms
                    status = "WARNING"
                    message = f"最大延迟过高: {max_latency:.2f}μs"
                else:
                    status = "HEALTHY"
                    message = f"延迟指标正常: 平均{avg_latency:.2f}μs"
                
                results.append(HealthCheckResult("latency", status, message, {
                    "avg_latency_us": avg_latency,
                    "max_latency_us": max_latency
                }))
        
        except Exception as e:
            results.append(HealthCheckResult("latency", "UNKNOWN", f"延迟检查失败: {str(e)}"))
        
        return results
    
    def _record_health_status(self, health_results: List[HealthCheckResult]):
        """记录健康状态"""
        try:
            for result in health_results:
                timestamp_str = result.timestamp.strftime("%Y.%m.%d %H:%M:%S.%f")[:-3]
                
                cpu_percent = result.details.get('cpu_percent', 0.0)
                memory_percent = result.details.get('memory_percent', 0.0)
                disk_percent = result.details.get('disk_percent', 0.0)
                
                self.session.run(f"""
                    insert into system_health_status values(
                        {timestamp_str}, `{result.component}, `{result.status}, 
                        "{result.message}", {cpu_percent}, {memory_percent}, {disk_percent}
                    )
                """)
        
        except Exception as e:
            print(f"记录健康状态失败: {e}")
    
    def generate_health_report(self, output_file: str = "health_report.json"):
        """生成健康报告"""
        health_results = self.perform_health_checks()
        
        report = {
            'report_time': datetime.now().isoformat(),
            'overall_status': self._calculate_overall_status(health_results),
            'component_status': [result.to_dict() for result in health_results],
            'recent_alerts': self.alert_manager.alert_history[-10:],  # 最近10个警报
            'system_summary': self._generate_system_summary()
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"健康报告已保存到: {output_file}")
        return report
    
    def _calculate_overall_status(self, health_results: List[HealthCheckResult]) -> str:
        """计算整体状态"""
        if any(r.status == "CRITICAL" for r in health_results):
            return "CRITICAL"
        elif any(r.status == "WARNING" for r in health_results):
            return "WARNING"
        else:
            return "HEALTHY"
    
    def _generate_system_summary(self) -> Dict:
        """生成系统摘要"""
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent()
        
        return {
            'uptime_hours': (datetime.now() - datetime.fromtimestamp(psutil.boot_time())).total_seconds() / 3600,
            'cpu_usage_percent': cpu_percent,
            'memory_usage_percent': memory.percent,
            'total_memory_gb': memory.total / (1024**3),
            'available_memory_gb': memory.available / (1024**3)
        }


class RecoveryManager:
    """自动恢复管理器"""
    
    def __init__(self, session):
        self.session = session
        self.recovery_actions = {}
        self.setup_recovery_actions()
    
    def setup_recovery_actions(self):
        """设置恢复动作"""
        self.recovery_actions = {
            'high_memory_usage': self._recover_memory,
            'ddb_connection_failed': self._recover_connection,
            'stream_engine_stopped': self._recover_stream_engines
        }
    
    def attempt_recovery(self, health_results: List[HealthCheckResult], alerts: List[Dict]):
        """尝试自动恢复"""
        for alert in alerts:
            rule_name = alert['rule_name']
            
            if rule_name in self.recovery_actions:
                try:
                    print(f"尝试自动恢复: {rule_name}")
                    success = self.recovery_actions[rule_name](health_results, alert)
                    
                    if success:
                        print(f"自动恢复成功: {rule_name}")
                    else:
                        print(f"自动恢复失败: {rule_name}")
                
                except Exception as e:
                    print(f"自动恢复异常: {rule_name}, 错误: {e}")
    
    def _recover_memory(self, health_results: List[HealthCheckResult], alert: Dict) -> bool:
        """内存恢复"""
        try:
            # 清理DolphinDB缓存
            self.session.run("""
                clearAllCache()
                gc()
            """)
            
            # 清理流表历史数据
            self.session.run("""
                cutoff_time = now() - 3600000  // 保留1小时数据
                if(existsTable("tickStream")){
                    delete from tickStream where timestamp < cutoff_time
                }
                if(existsTable("factor_stream")){
                    delete from factor_stream where timestamp < cutoff_time
                }
            """)
            
            return True
        
        except Exception as e:
            print(f"内存恢复失败: {e}")
            return False
    
    def _recover_connection(self, health_results: List[HealthCheckResult], alert: Dict) -> bool:
        """连接恢复"""
        try:
            # 重新连接DolphinDB
            self.session.close()
            time.sleep(5)
            self.session.connect("localhost", 8848, "admin", "123456")
            
            # 测试连接
            result = self.session.run("1+1")
            return result == 2
        
        except Exception as e:
            print(f"连接恢复失败: {e}")
            return False
    
    def _recover_stream_engines(self, health_results: List[HealthCheckResult], alert: Dict) -> bool:
        """流引擎恢复"""
        try:
            # 重启停止的流引擎
            self.session.run("""
                // 这里需要根据实际的引擎名称来重启
                // 示例代码，需要根据具体情况调整
            """)
            
            return True
        
        except Exception as e:
            print(f"流引擎恢复失败: {e}")
            return False


if __name__ == "__main__":
    # 测试系统健康监控
    monitor = SystemHealthMonitor()
    
    try:
        # 添加邮件通知渠道（示例）
        # monitor.alert_manager.add_notification_channel("email", {
        #     'smtp_server': 'smtp.gmail.com',
        #     'smtp_port': 587,
        #     'use_tls': True,
        #     'username': 'your_email@gmail.com',
        #     'password': 'your_password',
        #     'from_email': 'your_email@gmail.com',
        #     'to_email': 'admin@company.com'
        # })
        
        # 开始监控
        monitor.start_monitoring(interval=10.0)  # 10秒检查一次
        
        print("系统健康监控已启动，按 Ctrl+C 停止...")
        
        # 保持运行
        while True:
            time.sleep(60)
            
            # 每分钟生成一次健康报告
            monitor.generate_health_report("performance_testing/health_report.json")
    
    except KeyboardInterrupt:
        print("\n停止监控...")
        monitor.stop_monitoring()
    
    except Exception as e:
        print(f"监控过程中发生错误: {e}")
        monitor.stop_monitoring()
