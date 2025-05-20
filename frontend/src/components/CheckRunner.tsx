import { useEffect, useState } from "react";

type Rule = {
  rule_id: number;
  rule_name: string;
  rule_type: string;
  sql_text: string;
  rule_description?: string;
};

export default function CheckRunner() {
  const [connectionName, setConnectionName] = useState<string | null>(null);
  const [rules, setRules] = useState<Rule[]>([]);
  const [selectedRule, setSelectedRule] = useState<Rule | null>(null);
  const [tables, setTables] = useState<string[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [tableParams, setTableParams] = useState<{ [key: string]: string }>({});
  const [message, setMessage] = useState("");
  const [result, setResult] = useState<any>(null);

  useEffect(() => {
    const stored = localStorage.getItem("selectedConnection");
    setConnectionName(stored);
  }, []);

  useEffect(() => {
    fetch("/api/rules/")
      .then(res => res.json())
      .then(data => setRules(Array.isArray(data) ? data : []))
      .catch(() => setMessage("Ошибка загрузки правил"));
  }, []);

  useEffect(() => {
    if (!connectionName) return;
    fetch(`/api/tables?connection_name=${connectionName}`)
      .then(res => res.json())
      .then(data => setTables(Array.isArray(data) ? data : []))
      .catch(() => setMessage("Ошибка загрузки таблиц"));
  }, [connectionName]);

  const loadColumns = async (table: string) => {
    if (!connectionName || !table) return;
    const res = await fetch(`/api/columns?connection_name=${connectionName}&table=${table}`);
    const data = await res.json();
    if (Array.isArray(data)) setColumns(data);
  };

  const handleRunCheck = async () => {
    if (!selectedRule || !connectionName) {
      setMessage("Не выбрано правило");
      return;
    }

    setMessage("Запуск проверки...");
    setResult(null);

    const payload = {
      rule_name: selectedRule.rule_name,
      connection_name: connectionName,
      table_params: tableParams,
    };

    try {
      const res = await fetch("/api/trigger_dag", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const data = await res.json();

      if (!res.ok) {
        setMessage("Ошибка запуска DAG: " + (data.detail || "неизвестно"));
        return;
      }

      const resultId = data.result_id;
      setMessage("Проверка запущена. Ожидание результата...");
      await pollForResult(resultId);
    } catch (err) {
      setMessage("Ошибка отправки запроса: " + err);
    }
  };

  const pollForResult = async (resultId: number) => {
    for (let i = 0; i < 20; i++) {
      await new Promise(res => setTimeout(res, 2000));
      const res = await fetch(`/api/report/result/${resultId}`);
      if (res.ok) {
        const data = await res.json();
        setResult({ ...data, result_id: resultId });
        setMessage(`Проверка завершена: найдено ${data.error_count} ошибок`);
        return;
      }
    }
    setMessage("Истекло время ожидания результата");
  };

  const handleExport = async () => {
    if (!result?.result_id) {
      setMessage("Невозможно экспортировать: нет ID результата");
      return;
    }

    setMessage("Запрос экспорта...");
    const res = await fetch("/api/export/result/", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ result_id: result.result_id }),
    });

    if (!res.ok) {
      const err = await res.text();
      setMessage("Ошибка экспорта: " + err);
      return;
    }

    setMessage("Экспорт запущен, файл скоро будет готов...");
    await new Promise(r => setTimeout(r, 5000));
    window.location.href = `/api/export/result/download/${result.result_id}`;
    setTimeout(() => setMessage("Отчет загружен"), 1000);
  };
  return (
    <div style={{ backgroundColor: "#dae6f2", minHeight: "100vh", padding: "2rem", fontFamily: "sans-serif" }}>
      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: "1rem" }}>
        <a href="/" style={{ marginRight: "1rem", backgroundColor: "#4CAF50", color: "white", padding: "0.5rem 1rem", borderRadius: "4px", textDecoration: "none" }}>Подключения</a>
        <a href="/reports" style={{ backgroundColor: "#4CAF50", color: "white", padding: "0.5rem 1rem", borderRadius: "4px", textDecoration: "none" }}>Отчёты</a>
      </div>

      <div style={{
        maxWidth: 900,
        margin: "0 auto",
        backgroundColor: "white",
        padding: "2rem",
        borderRadius: "8px",
        boxShadow: "0 0 10px rgba(0,0,0,0.1)"
      }}>
        <h2 style={{ fontSize: "1.6rem", marginBottom: "1rem" }}>Запуск проверки</h2>
        <p><strong>Подключение:</strong> {connectionName || "не выбрано"}</p>

        <label>Выберите правило:</label><br />
        <select
          onChange={e => {
            const rule = rules.find(r => r.rule_name === e.target.value);
            setSelectedRule(rule || null);
            setTableParams({});
            setColumns([]);
          }}
          style={{ padding: "0.5rem", marginBottom: "1rem", width: "100%", maxWidth: 400 }}
        >
          <option value="">-- выбрать --</option>
          {rules.map(rule => (
            <option key={rule.rule_id} value={rule.rule_name}>{rule.rule_name}</option>
          ))}
        </select>

        {selectedRule?.rule_description && (
          <div style={{
            marginTop: "1rem",
            backgroundColor: "#f6f8fa",
            padding: "1rem",
            borderLeft: "4px solid green",
            borderRadius: "4px",
            whiteSpace: "pre-wrap"
          }}>
            <strong>Описание проверки:</strong><br />
            {selectedRule.rule_description}
          </div>
        )}

        {/* Поля выбора таблиц и колонок — остаются нетронутыми */}
        {selectedRule?.rule_type === "referential" && (
          <div style={{ display: "flex", flexWrap: "wrap", gap: "1rem", marginTop: "1rem" }}>
            <div>
              <label>Исходная таблица:</label><br />
              <select onChange={e => {
                const val = e.target.value;
                setTableParams(prev => ({ ...prev, source_table: val }));
                loadColumns(val);
              }}>
                <option value="">-- выбрать --</option>
                {tables.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div>
              <label>Исходное поле:</label><br />
              <select onChange={e =>
                setTableParams(prev => ({ ...prev, source_column: e.target.value }))
              }>
                <option value="">-- выбрать --</option>
                {columns.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
            <div>
              <label>Целевая таблица:</label><br />
              <select onChange={e => {
                const val = e.target.value;
                setTableParams(prev => ({ ...prev, target_table: val }));
                loadColumns(val);
              }}>
                <option value="">-- выбрать --</option>
                {tables.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div>
              <label>Целевое поле:</label><br />
              <select onChange={e =>
                setTableParams(prev => ({ ...prev, target_column: e.target.value }))
              }>
                <option value="">-- выбрать --</option>
                {columns.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
          </div>
        )}

        {selectedRule?.rule_type === "negative_flows_check" && (
          <div style={{ marginTop: "1rem" }}>
            <label>Таблица для проверки:</label><br />
            <select onChange={e => {
              const val = e.target.value;
              setTableParams({ source_table: val });
            }}>
              <option value="">-- выбрать --</option>
              {tables.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
        )}

        {selectedRule?.rule_type === "attribute_reference_check" && (
          <div style={{ display: "flex", flexDirection: "column", gap: "1rem", marginTop: "1rem" }}>
            <div>
              <label>Проверяемая таблица:</label><br />
              <select onChange={e => {
                const val = e.target.value;
                setTableParams(prev => ({ ...prev, source_table: val }));
              }}>
                <option value="">-- выбрать --</option>
                {tables.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div>
              <label>Справочник (таблица с валидными значениями):</label><br />
              <select onChange={e => {
                const val = e.target.value;
                setTableParams(prev => ({ ...prev, reference_table: val }));
              }}>
                <option value="">-- выбрать --</option>
                {tables.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
          </div>
        )}

        {selectedRule?.rule_type === "null_check" && (
          <div style={{ marginTop: "1rem" }}>
            <label>Проверяемая таблица:</label><br />
            <select onChange={e => {
              const val = e.target.value;
              setTableParams({ source_table: val });
            }}>
              <option value="">-- выбрать --</option>
              {tables.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
        )}

        {selectedRule?.rule_type === "duplicate" && (
          <div style={{ marginTop: "1rem", display: "flex", flexDirection: "column", gap: "1rem" }}>
            <div>
              <label>Таблица:</label><br />
              <select onChange={e => {
                const val = e.target.value;
                setTableParams(prev => ({ ...prev, source_table: val }));
                loadColumns(val);
              }}>
                <option value="">-- выбрать --</option>
                {tables.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div>
              <label>Проверяемое поле:</label><br />
              <select onChange={e =>
                setTableParams(prev => ({ ...prev, check_column: e.target.value }))
              }>
                <option value="">-- выбрать --</option>
                {columns.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
          </div>
        )}

        <div style={{ marginTop: "1.5rem" }}>
          <button
            onClick={handleRunCheck}
            style={{
              padding: "0.6rem 1.2rem",
              fontSize: "1rem",
              backgroundColor: "#4CAF50",
              color: "white",
              border: "none",
              borderRadius: "4px",
              cursor: "pointer",
              marginRight: "1rem"
            }}
          >
            🚀 Запустить проверку
          </button>

          {result && (
            <button
              onClick={handleExport}
              style={{
                padding: "0.6rem 1.2rem",
                fontSize: "1rem",
                backgroundColor: "#2196F3",
                color: "white",
                border: "none",
                borderRadius: "4px",
                cursor: "pointer"
              }}
            >
              📥 Экспорт в Excel
            </button>
          )}
        </div>

        {message && <p style={{ marginTop: "1rem", color: message.includes("Ошибка") ? "red" : "green" }}>{message}</p>}

        {result && result.rows?.length > 0 && (
          <div style={{ marginTop: "2rem" }}>
            <h4>Результаты последней проверки:</h4>
            <table border={1} cellPadding={6} style={{ marginTop: "1rem", borderCollapse: "collapse", width: "100%" }}>
              <thead>
                <tr style={{ backgroundColor: "#f0f0f0" }}>
                  <th>Таблица</th>
                  <th>Правило</th>
                  <th>Запись</th>
                  <th>Описание</th>
                </tr>
              </thead>
              <tbody>
                {result.rows.map((r: any, i: number) => (
                  <tr key={i}>
                    <td>{r.table_name}</td>
                    <td>{r.rule_name}</td>
                    <td>{r.record_reference}</td>
                    <td>{r.error_description}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
