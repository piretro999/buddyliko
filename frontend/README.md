# 🌍 Buddyliko

**Transform Your Data**

> *Inspired by the Swahili word "Badiliko" (change/transformation)*

Buddyliko is a visual data mapping and transformation platform that makes it easy to map, transform, and convert data between different formats and schemas.

---

## ✨ The Name

**Buddyliko** combines two powerful concepts:

- **Buddy**: Friendly, accessible, your trusted data companion
- **Badiliko**: Swahili for "change" or "transformation"

Perfect for a tool that transforms data mappings with ease and confidence!

---

## 🚀 Features

- **Visual Schema Mapping**: Drag-and-drop interface for creating data mappings
- **Multi-Format Support**: XML, JSON, CSV, IDOC, and more
- **Real-Time Transformation**: See results as you map
- **Authentication**: Secure login with email/password or OAuth (Google, Facebook, GitHub)
- **Cloud Storage**: SQLite, TinyDB, or PostgreSQL backends
- **API-First**: Complete REST API with FastAPI

---

## 🎨 Brand Identity

### Colors (Transformation Palette)
- **Primary**: `#2196F3` (Blue Ocean)
- **Secondary**: `#4CAF50` (Energy Green)
- **Gradient**: Blue → Green (symbolizing data transformation)
- **Accent**: `#FF9800` (Orange)

### Logo
The Buddyliko logo features circular transformation arrows representing continuous change and data flow.

---

## 📦 Installation

### Prerequisites
- Python 3.8+
- pip

### Quick Start

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure** (optional)
   Edit `config.yml` to enable authentication, configure OAuth, or change database settings.

3. **Run**
   ```bash
   # Windows
   run.bat
   
   # Linux/Mac
   python -m uvicorn backend.api:app --host 127.0.0.1 --port 8080
   ```

4. **Open** your browser
   ```
   http://localhost:8000
   ```

---

## 🔐 Authentication

Buddyliko supports multiple authentication methods:

- **Email/Password**: Local registration and login
- **Google OAuth**: Sign in with Google account
- **Facebook OAuth**: Sign in with Facebook account  
- **GitHub OAuth**: Sign in with GitHub account

Configure in `config.yml`:
```yaml
auth:
  enabled: true
  oauth:
    google:
      enabled: true
      client_id: "YOUR_CLIENT_ID"
      client_secret: "YOUR_SECRET"
```

---

## 🛠️ Tech Stack

- **Backend**: FastAPI (Python)
- **Frontend**: React (via CDN), Vanilla JavaScript
- **Database**: SQLite / TinyDB / PostgreSQL
- **Auth**: JWT tokens, OAuth 2.0
- **Data Processing**: pandas, lxml, openpyxl

---

## 📚 Documentation

- **API Docs**: http://localhost:8080/docs (Swagger UI)
- **Brand Guide**: See [BRAND_GUIDE.md](BRAND_GUIDE.md)
- **Configuration**: See [config.yml](config.yml)

---

## 🌍 Etymology

The name "Buddyliko" is inspired by **Badiliko**, a Swahili word meaning "change" or "transformation." 

Swahili is a Bantu language widely spoken in East Africa, and the word perfectly captures what this platform does: transforming data from one format to another with ease and confidence.

---

## 🎯 Use Cases

- **Data Migration**: Convert between different data formats
- **API Integration**: Map data between systems
- **EDI Processing**: Handle IDOC, X12, EDIFACT formats
- **Schema Transformation**: XML → JSON, CSV → XML, etc.
- **Data Normalization**: Standardize data across sources

---

## 📄 License

See LICENSE file for details.

---

## 🙏 Acknowledgments

Built with ❤️ using:
- FastAPI by Sebastián Ramírez
- React by Meta
- Icons from Lucide
- Inspired by the Swahili language and East African culture

---

## 🔗 Links

- **Repository**: [GitHub](#)
- **Issues**: [GitHub Issues](#)
- **Documentation**: [Docs](#)

---

<div align="center">

**🌍 Buddyliko** - *Transform Your Data*

*Badiliko (Swahili) = Change*

</div>
