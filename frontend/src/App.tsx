import { BrowserRouter as Router, Routes, Route, Link } from "react-router-dom";
import Home from "./pages/Home";
import Reports from "./pages/Reports";
import CheckRunner from "./components/CheckRunner";

function App() {
  return (
    <Router>
      <nav style={{ padding: "10px" }}>
        <Link to="/">Проверки</Link> | <Link to="/reports">Отчёты</Link>
      </nav>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/reports" element={<Reports />} />
        <Route path="/checks" element={<CheckRunner />} />
      </Routes>
    </Router>
  );
}

export default App;
