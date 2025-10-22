# Proyek Akhir: Membangun ETL Pipeline Sederhana

### Ringkasan

Proyek ini membangun pipeline ETL modular (extract, transform, load) untuk mengekstrak data produk, membersihkannya, lalu memuat hasil ke repositori data yang mudah diakses kembali. Proses memanfaatkan scraping berbasis Requests/Beautiful Soup untuk ekstraksi, transformasi tipe dan validasi data, serta penyimpanan ke CSV, Google Sheets, dan/atau PostgreSQL. Kualitas dijaga dengan unit test dan laporan coverage menggunakan pytest serta plugin pytest-cov agar mudah diverifikasi.

### Struktur proyek

Struktur berikut memisahkan tiap tahap ETL ke modul terpisah untuk kemudahan pengujian dan pemeliharaan.

```
submission-pemda
├── tests
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── utils
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── main.py
├── requirements.txt
├── submission.txt
├── products.csv
└── google-sheets-api.json
```

Sertakan requirements.txt untuk menyamakan dependency lintas lingkungan pengujian dan review.

### Persiapan

- Gunakan virtual environment, lalu instal dependensi: pandas, requests, beautifulsoup4, gspread, google-auth, psycopg2-binary, pytest, dan pytest-cov.
- Jika memakai Google Sheets, simpan kredensial service account sebagai berkas JSON dan bagikan spreadsheet ke email service account dengan izin Editor.
- Pastikan Python dapat menangani error dan logging dengan baik agar mudah didiagnosis saat scraping dan I/O jaringan.

### Menjalankan

- Atur konfigurasi seperti target repositori (CSV/Sheets/PostgreSQL), path kredensial, nama sheet, atau DSN database melalui environment variable atau berkas konfigurasi.
- Eksekusi pipeline melalui main.py untuk menjalankan urutan extract → transform → load sesuai kebutuhan.
- Jalankan pengujian dan coverage menggunakan pytest dan pytest-cov untuk memverifikasi kualitas dan menetapkan ambang minimal cakupan.

### ETL pipeline

- Extract: lakukan scraping multi-halaman dan ambil Title, Price, Rating, Colors, Size, serta Gender dari setiap entri menggunakan Requests + Beautiful Soup.
- Transform: bersihkan null, duplikat, dan nilai tidak valid; normalisasikan tipe data; konversi Price USD→IDR dengan kurs tetap yang terdokumentasi agar reprodusibel.
- Load: simpan hasil akhir ke CSV, dan opsional muat ke Google Sheets atau PostgreSQL untuk konsumsi tim data lain.

### Validasi data

- Pastikan tidak ada nilai invalid seperti placeholder nama produk, serta tidak ada nilai null atau duplikat setelah transformasi.
- Normalisasi tipe dan format: Rating float; Colors angka; Size dan Gender string tanpa label tambahan agar siap analisis.

### Repositori data

- CSV: ekspor hasil final ke products.csv menggunakan pandas.DataFrame.to_csv untuk interoperabilitas lintas alat.
- Google Sheets: gunakan gspread dengan service account dan pastikan akun layanan memiliki izin Editor pada spreadsheet target.
- PostgreSQL: gunakan psycopg2 dengan query terparametrisasi dan komit transaksi untuk batch insert yang aman dan efisien.

### Pengujian

- Simpan seluruh pengujian di folder tests dan uji fungsi per tahap ETL (extract/transform/load) agar area kritis tercakup.
- Gunakan opsi --cov, --cov-report, dan --cov-fail-under untuk menghasilkan laporan dan menegakkan ambang coverage proyek.

### Penanganan error

- Terapkan try/except spesifik per fungsi di extract.py, transform.py, dan load.py, sertakan logging, serta gunakan finally untuk pembersihan resource.
- Jaga blok try kecil dan tangkap exception spesifik guna mencegah penanganan kesalahan yang terlalu umum.

### Checklist kelulusan

- [ ] Kode modular: utils/extract.py, utils/transform.py, utils/load.py, dengan entry point di main.py.
- [ ] Data hasil ekstraksi mencakup Title, Price, Rating, Colors, Size, Gender dari seluruh halaman target.
- [ ] Transformasi menghapus null/duplikat/invalid; normalisasi tipe; dan konversi kurs Price sesuai konfigurasi.
- [ ] Hasil bersih disimpan ke CSV dan, bila diperlukan, ke Google Sheets/PostgreSQL dengan kredensial benar.
- [ ] Seluruh unit test berada di tests dan coverage ditegakkan dengan pytest-cov.
- [ ] Setiap fungsi utama memiliki penanganan error yang jelas dan teruji.

### Contoh snippet

Simpan ke CSV menggunakan pandas.DataFrame.to_csv untuk ekspor yang konsisten lintas alat analitik.

```python
df.to_csv("products.csv", index=False)
```

Tulis ke Google Sheets menggunakan gspread service_account dan update nilai secara batch.

```python
import gspread
gc = gspread.service_account(filename="google-sheets-api.json")
sh = gc.open("nama-spreadsheet").sheet1
sh.update([df.columns.values.tolist()] + df.values.tolist())
```

Insert batch ke PostgreSQL dengan psycopg2.execute_values agar efisien.

```python
import psycopg2, psycopg2.extras as ex
conn = psycopg2.connect(dsn)
with conn, conn.cursor() as cur:
    ex.execute_values(
        cur,
        "INSERT INTO products (title, price_idr, rating, colors, size, gender) VALUES %s",
        list(df[["Title","Price_IDR","Rating","Colors","Size","Gender"]].itertuples(index=False, name=None))
    )
```

### Catatan

Dokumentasikan parameter penting (kurs konversi, batas paginasi, target coverage) di README dan sebagai ENV untuk memastikan reprodusibilitas serta kemudahan review. Gunakan laporan coverage HTML untuk mengidentifikasi baris yang belum teruji dan memandu prioritas penulisan test tambahan.

| Platform | Shell      | Command untuk mengaktifkan virtual environment |
| -------- | ---------- | ---------------------------------------------- |
| POSIX    | bash/zsh   | $ source .env/bin/activate                     |
| POSIX    | fish       | $ source .env/bin/activate.fish                |
| POSIX    | csh/tcsh   | $ source .env/bin/activate.csh                 |
| POSIX    | pwsh       | $ .env/bin/Activate.ps1                        |
| Windows  | cmd.exe    | C:\> .env\Scripts\activate.bat                 |
| Windows  | PowerShell | PS C:\> .env\Scripts\Activate.ps1              |
