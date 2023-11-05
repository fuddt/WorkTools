import tkinter as tk
import os
from apache_log import ApacheLog
from apache_log_analys import ApacheLogAnalys
from tkinter import *
from tkinter import filedialog, Text


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Test")
        self.geometry("900x800")
        self.resizable(True, True)
                
        # File path variable
        self.read_log_file_path = None
        self.output_directory_path = None
        self.read_csv_file_path = None

        # Groupby variable
        self.time = None
        self.col = None
        self.groupby_columns = None
        self.target_columns = None
        self.calc_count = None
        self.calc_sum = None
        self.calc_mean = None
        self.calc_max = None
        self.calc_min = None
        self.calc_std = None

        # Read Log File Button
        self.read_log_file_button = tk.Button(self, text="Read Log File", command=self.read_log_file)
        self.read_log_file_button.grid(row=0, column=0, padx=10, pady=10)
        # Read Log File Path Entry
        self.read_log_file_path_entry = tk.Entry(self)
        self.read_log_file_path_entry.grid(row=0, column=1, padx=10, pady=10)

        # Day Entry
        self.day_entry_label = tk.Label(self, text="Day:")
        self.day_entry_label.grid(row=1, column=0, padx=10, pady=10)
        self.day_entry = tk.Entry(self)
        self.day_entry.grid(row=1, column=1, padx=10, pady=10)

        # From Time Entry
        self.from_time_entry_label = tk.Label(self, text="From Time:")
        self.from_time_entry_label.grid(row=2, column=0, padx=10, pady=10)
        self.from_time_entry = tk.Entry(self)
        self.from_time_entry.grid(row=2, column=1, padx=10, pady=10)

        # To Time Entry
        self.to_time_entry_label = tk.Label(self, text="To Time:")
        self.to_time_entry_label.grid(row=3, column=0, padx=10, pady=10)
        self.to_time_entry = tk.Entry(self)
        self.to_time_entry.grid(row=3, column=1, padx=10, pady=10)

        # Output Directory Button
        self.output_directory_button = tk.Button(self, text="Output Directory", command=self.output_directory)
        self.output_directory_button.grid(row=4, column=0, padx=10, pady=10)
        # Output Directory Path Entry
        self.output_directory_path_entry = tk.Entry(self)
        self.output_directory_path_entry.grid(row=4, column=1, padx=10, pady=10)

        # Apache Log To Csv Button
        self.apache_log_to_csv_button = tk.Button(self, text="Apache Log To Csv", command=self.apache_log_to_csv)
        self.apache_log_to_csv_button.grid(row=5, column=0, padx=10, pady=10)

        # separate information by time
        self.time_entry_label = tk.Label(self, text="Time:")
        self.time_entry_label.grid(row=6, column=0, padx=10, pady=10)
        self.time_entry = tk.Entry(self)
        self.time_entry.grid(row=6, column=1, padx=10, pady=10)
        self.col_entry_label = tk.Label(self, text="Col:")
        self.col_entry_label.grid(row=7, column=0, padx=10, pady=10)
        self.col_entry = tk.Entry(self)
        self.col_entry.grid(row=7, column=1, padx=10, pady=10)
        self.groupby_columns_entry_label = tk.Label(self, text="Groupby Columns:")
        self.groupby_columns_entry_label.grid(row=8, column=0, padx=10, pady=10)
        self.groupby_columns_entry = tk.Entry(self)
        self.groupby_columns_entry.grid(row=8, column=1, padx=10, pady=10)
        self.target_columns_entry_label = tk.Label(self, text="Target Columns:")
        self.target_columns_entry_label.grid(row=8, column=2, padx=10, pady=10)
        self.target_columns_entry = tk.Entry(self)
        self.target_columns_entry.grid(row=8, column=3, padx=10, pady=10)

        # count mean max min std sum Checkbox
        self.calc_count = tk.IntVar()
        self.calc_sum = tk.IntVar()
        self.calc_mean = tk.IntVar()
        self.calc_max = tk.IntVar()
        self.calc_min = tk.IntVar()
        self.calc_std = tk.IntVar()
        self.calc_count_checkbox = tk.Checkbutton(self, text="count", variable=self.calc_count)
        self.calc_count_checkbox.grid(row=9, column=0, padx=10, pady=10)
        self.calc_sum_checkbox = tk.Checkbutton(self, text="sum", variable=self.calc_sum)
        self.calc_sum_checkbox.grid(row=9, column=1, padx=10, pady=10)
        self.calc_mean_checkbox = tk.Checkbutton(self, text="mean", variable=self.calc_mean)
        self.calc_mean_checkbox.grid(row=9, column=2, padx=10, pady=10)
        self.calc_max_checkbox = tk.Checkbutton(self, text="max", variable=self.calc_max)
        self.calc_max_checkbox.grid(row=9, column=3, padx=10, pady=10)
        self.calc_min_checkbox = tk.Checkbutton(self, text="min", variable=self.calc_min)
        self.calc_min_checkbox.grid(row=9, column=4, padx=10, pady=10)
        self.calc_std_checkbox = tk.Checkbutton(self, text="std", variable=self.calc_std)
        self.calc_std_checkbox.grid(row=9, column=5, padx=10, pady=10)

        # Apache Csv File Button
        self.read_csv_file_button = tk.Button(self, text="Read Csv File", command=self.read_csv_file)
        self.read_csv_file_button.grid(row=10, column=0, padx=10, pady=10)
        # Apache Csv File Path Entry
        self.read_csv_file_path_entry = tk.Entry(self)
        self.read_csv_file_path_entry.grid(row=10, column=1, padx=10, pady=10)

        # apache log calculate button
        self.apache_log_calculate_button = tk.Button(self, text="Apache Log Calculate", command=self.apache_log_calculate)
        self.apache_log_calculate_button.grid(row=11, column=0, padx=10, pady=10)



    def read_log_file(self):
        self.read_log_file_path = filedialog.askopenfilename(initialdir="/", title="Select log file")
        self.read_log_file_path_entry.insert(0, self.read_log_file_path)

    def output_directory(self):
        self.output_directory_path = filedialog.askdirectory(initialdir="/", title="Select output directory")
        self.output_directory_path_entry.insert(0, self.output_directory_path)

    def read_csv_file(self):
        self.read_csv_file_path = filedialog.askopenfilename(initialdir="/", title="Select csv file", filetypes=(("csv files", "*.csv"), ("all files", "*.*")))
        self.read_csv_file_path_entry.insert(0, self.read_csv_file_path)

    def apache_log_to_csv(self):
        day = self.day_entry.get()
        from_time = self.from_time_entry.get()
        to_time = self.to_time_entry.get()
        
        apache_log = ApacheLog(self.read_log_file_path)
        apache_log.filter_by_day(day)
        apache_log.filter_by_time(from_time, to_time)
        apache_log.to_csv(os.path.join(self.output_directory_path,self.read_log_file_path+".csv"))

    def apache_log_calculate(self):
        apache_log_analys = ApacheLogAnalys(self.read_csv_file_path)
        time = self.time_entry.get()
        col = self.col_entry.get()
        apache_log_analys.separate_by_time(time, col)
        groupby_columns = self.groupby_columns_entry.get()
        target_columns = self.target_columns_entry.get()
        apache_log_analys.calculate_data(groupby_columns, 
                                         target_columns, 
                                         self.calc_count.get(), 
                                         self.calc_sum.get(), 
                                         self.calc_mean.get(), 
                                         self.calc_max.get(), 
                                         self.calc_min.get(), 
                                         self.calc_std.get())
        apache_log_analys.to_csv(self.read_csv_file_path.replace(".csv","")+"Result.csv")


    def run(self):
        self.mainloop()

if __name__ == "__main__":
    app = App()
    app.run()