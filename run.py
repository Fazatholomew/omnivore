from dashboard import app

DEBUG = app.config["DEBUG"]

app.logger.info("DEBUG            = " + str(DEBUG))
app.logger.info("Page Compression = " + "FALSE" if DEBUG else "TRUE")
app.logger.info("ASSETS_ROOT      = " + app.config["ASSETS_ROOT"])

if __name__ == "__main__":
    app.run()
