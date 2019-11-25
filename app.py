from flask import Flask, render_template, Markup
from xml_tools import get_xml, insert_xml, scrap_sat, query_xml, load_xml

app=Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/reporte")
def reporte():
    a, b, c = query_xml()
    a_html = Markup(a)
    b_html = Markup(b)
    c_html = Markup(c)

    return render_template("reporte.html", t1=a_html, t2=b_html, t3=c_html)

if __name__=="__main__":
    app.run(debug=True)