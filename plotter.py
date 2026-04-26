# This module constructs the Plotter class

##
# This module defines the Plotter class, used as a helper for the model scripts
# It renders styled PDF tables for summarising results, to ensure consistent formatting across all models
##

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt

# Shared plot and table rendering
class Plotter:

    # Class-level style constants — same header/row colours used in all tables
    HEADER_COLOR   = "#2c3e50"
    ROW_EVEN_COLOR = "#f0f4f8"

    # Transforms target column names into a safe file name by replacing problematic characters
    def safe_filename(self, target):
        return target.replace("/", "-").replace(" ", "_").replace("∆", "d")

    # Creates a styled PDF table from a DataFrame
    def results_table(self, disp, title, save_path, width=16):
        height  = len(disp) * 0.55 + 2.5
        fig, ax = plt.subplots(figsize=(width, height))
        ax.axis("off")
        ax.set_title(title, fontsize=13, fontweight="bold", pad=20)

        table = ax.table(
            cellText=disp.values,
            colLabels=disp.columns,
            loc="center",
            cellLoc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.8)

        for j in range(len(disp.columns)):
            table[0, j].set_facecolor(self.HEADER_COLOR)
            table[0, j].set_text_props(color="white", fontweight="bold")

        for i in range(1, len(disp) + 1):
            color = self.ROW_EVEN_COLOR if i % 2 == 0 else "white"
            for j in range(len(disp.columns)):
                table[i, j].set_facecolor(color)

        plt.savefig(save_path, format="pdf", bbox_inches="tight", dpi=150)
        plt.close()
        print(f"PDF saved → {save_path}")
