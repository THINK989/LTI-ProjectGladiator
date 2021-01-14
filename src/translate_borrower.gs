function myFunction() {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = ss.getSheets()[0];

    // This represents ALL the data
    var range = sheet.getDataRange();
    var values = range.getValues();

    // This logs the spreadsheet in CSV format with a trailing comma
    for (var i = 0; i < values.length; i++) {
        var row = "";
        for (var j = 0; j < values[i].length; j++) {
            if (values[i][j]) {
                var temp=String(values[i][j]);
                temp=LanguageApp.translate(temp,'','en');
                sheet.appendRow([temp]);
            }
        }
    }
}