# etl/extract/extract_ui.py

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
from etl.extract.extractor import extract_data, detect_file_type



class ExtractUI:
    def __init__(self, root):
        self.root = root
        root.title("Dynamic ETL - Extract Layer Tester")
        root.geometry("800x600")

        # Title
        tk.Label(root, text="📥 Extract Layer Testing UI",
                 font=("Arial", 18, "bold")).pack(pady=10)

        # Browse Button
        tk.Button(root, text="Browse File", command=self.browse_file,
                  font=("Arial", 12), width=20).pack(pady=10)

        # File Path Label
        self.file_label = tk.Label(root, text="No file selected", font=("Arial", 10))
        self.file_label.pack()

        # Info Box
        self.info_box = tk.Label(root, text="", font=("Arial", 12), fg="blue")
        self.info_box.pack(pady=10)

        # Table Frame
        self.table_frame = ttk.Frame(root)
        self.table_frame.pack(fill="both", expand=True)

    def browse_file(self):
        file_path = filedialog.askopenfilename(
            title="Select a File",
            filetypes=[
                ("All Supported", "*.json *.csv *.txt *.html *.xlsx *.xls *.tsv *.xml"),
                ("JSON files", "*.json"),
                ("CSV files", "*.csv"),
                ("Text files", "*.txt"),
                ("HTML files", "*.html"),
                ("Excel files", "*.xlsx *.xls"),
                ("TSV files", "*.tsv"),
                ("XML files", "*.xml"),
            ]
        )

        if not file_path:
            return

        self.file_label.config(text=file_path)

        try:
            file_type = detect_file_type(file_path)
            df = extract_data(file_path)

            if df.empty:
                self.info_box.config(text="⚠️ No data extracted.")
                return

            self.info_box.config(text=f"✔ {file_type.upper()} file loaded — {len(df)} records")

            self.show_table(df)

        except Exception as e:
            messagebox.showerror("Extraction Error", str(e))

    def show_table(self, df):
        # Clear old table
        for widget in self.table_frame.winfo_children():
            widget.destroy()

        # Table widget
        table = ttk.Treeview(self.table_frame)
        table.pack(fill="both", expand=True)

        # Columns
        table["columns"] = list(df.columns)
        table["show"] = "headings"

        for col in df.columns:
            table.heading(col, text=col)
            table.column(col, width=150)

        # Insert first 20 rows
        for _, row in df.head(20).iterrows():
            table.insert("", tk.END, values=list(row))


if __name__ == "__main__":
    root = tk.Tk()
    app = ExtractUI(root)
    root.mainloop()
