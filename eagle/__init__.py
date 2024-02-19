import os
from flask import Flask, jsonify, request
from sqlalchemy import select, delete, text, asc, insert
from config import Config
from eagle.extensions import db
from eagle.models.raw_dsp import RawDsp
from werkzeug.utils import secure_filename
from eagle.models.raw_comparison import RawComparison
import pandas as pd


def create_app(config_class=Config):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(config_class)

    # check upload folder exists or not
    if not os.path.isdir(app.config["UPLOAD_FOLDER"]):
        os.mkdir(app.config["UPLOAD_FOLDER"])

    # Initialize Flask extension
    db.init_app(app)

    @app.route("/upload-dsp", methods=["POST"])
    def upload_dsp():
        if "dsp_file" not in request.files:
            resp = {"status": 400, "message": "dsp_file should not be empty"}
            return jsonify(resp), 400
        dsp_file = request.files["dsp_file"]
        filename = dsp_file.filename
        if not filename:
            resp = {"status": 400, "message": "no selected file"}
            return jsonify(resp), 400
        if filename.lower().rsplit(".", 1)[1] != "xls":
            resp = {"status": 400, "message": "only accept xls file"}
            return jsonify(resp), 400

        form_data = request.form.to_dict()
        if form_data.get("satuankerja_id") is None or not (
            form_data.get("satuankerja_id")
        ):
            resp = {"status": 400, "message": "satuankerja_id should not be empty"}
            return jsonify(resp), 400
        if form_data.get("satuankerja_nama") is None or not (
            form_data.get("satuankerja_nama")
        ):
            resp = {"status": 400, "message": "satuankerja_nama should not be empty"}
            return jsonify(resp), 400
        if form_data.get("nomor_keputusan_kasau") is None or not (
            form_data.get("nomor_keputusan_kasau")
        ):
            resp = {
                "status": 400,
                "message": "nmmor_keputusan_kasau should not be empty",
            }
            return jsonify(resp), 400

        delete_stmt = delete(RawDsp).where(
            RawDsp.dsp_nomor_keputusan_kasau == form_data.get("nomor_keputusan_kasau")
        )
        db.session.execute(delete_stmt)
        db.session.commit()

        dsp_file.save(
            os.path.join(app.config["UPLOAD_FOLDER"], secure_filename(filename))
        )

        dsp_file_path = app.config["UPLOAD_FOLDER"] + secure_filename(filename)
        df = pd.read_excel(dsp_file_path, dtype=str)
        df.rename(
            columns={
                "Unnamed: 0": "dsp_nomor",
                "Unnamed: 1": "dsp_jabatan",
                "Unnamed: 2": "dsp_gol_jab",
                "Unnamed: 3": "dsp_pangkat",
                "Unnamed: 4": "dsp_korps",
                "Unnamed: 5": "dsp_bidang_profesi",
                "Unnamed: 6": "dsp_spesialisasi",
                "Unnamed: 7": "dsp_pa",
                "Unnamed: 8": "dsp_ba",
                "Unnamed: 9": "dsp_ta",
                "Unnamed: 10": "dsp_pns",
                "Unnamed: 11": "dsp_jml",
                "Unnamed: 12": "dsp_ket",
            },
            inplace=True,
        )
        df["dsp_pa"] = df["dsp_pa"].str.strip()
        df.dropna(how="all", subset=["dsp_jabatan"], inplace=True)
        df.query(
            "dsp_jabatan != 'J A B A T A N' and dsp_jabatan != 'JUMLAH' and dsp_jabatan != '2'",
            inplace=True,
        )
        df["dsp_jabatan"] = df["dsp_jabatan"].str.strip().str.lower()
        df["dsp_pangkat"] = df["dsp_pangkat"].replace("\n", " ", regex=True)
        df["dsp_korps"] = df["dsp_korps"].replace("\n", " ", regex=True)
        df["dsp_bidang_profesi"] = df["dsp_bidang_profesi"].replace(
            "\n", " ", regex=True
        )
        df["dsp_spesialisasi"] = df["dsp_spesialisasi"].replace("\n", " ", regex=True)
        df["dsp_ket"] = df["dsp_ket"].replace("\n", " ", regex=True)
        df["dsp_satuankerja_id"] = form_data.get("satuankerja_id")
        df["dsp_satuankerja_nama"] = form_data.get("satuankerja_nama")
        df["dsp_nomor_keputusan_kasau"] = form_data.get("nomor_keputusan_kasau")
        df["dsp_nomor"] = df["dsp_nomor"].replace(" ", None)
        df["dsp_pa"] = df["dsp_pa"].replace(r"\D+", None)
        df["dsp_ba"] = df["dsp_ba"].replace(r"\D+", None)
        df["dsp_ta"] = df["dsp_ta"].replace(r"\D+", None)
        df["dsp_pns"] = df["dsp_pns"].replace(r"\D+", None)
        df["dsp_pa"] = df["dsp_pa"].replace("", None)
        df["dsp_ba"] = df["dsp_ba"].replace("", None)
        df["dsp_ta"] = df["dsp_ta"].replace("", None)
        df["dsp_pns"] = df["dsp_pns"].replace("", None)
        df["dsp_pa"] = df["dsp_pa"].replace(" ", None)
        df["dsp_ba"] = df["dsp_ba"].replace(" ", None)
        df["dsp_ta"] = df["dsp_ta"].replace(" ", None)
        df["dsp_pns"] = df["dsp_pns"].replace(" ", None)
        df.astype(
            {
                "dsp_pa": "float",
                "dsp_ba": "float",
                "dsp_ta": "float",
                "dsp_pns": "float",
            }
        )

        df.to_sql(
            name="raw_dsp",
            con=db.engine,
            index=False,
            if_exists="append",
            schema="raw",
        )
        raw_dsp_stmt = (
            select(RawDsp)
            .where(
                RawDsp.dsp_nomor_keputusan_kasau
                == form_data.get("nomor_keputusan_kasau")
            )
            .where(RawDsp.dsp_satuankerja_id == form_data.get("satuankerja_id"))
            .order_by(asc(RawDsp.id))
        )
        raw_dsp_data = db.session.execute(raw_dsp_stmt)
        dsp_jabatan_list = []
        counter = 0
        jabatan_nama_list = []
        for jbt in raw_dsp_data.scalars():
            jabatan_nama_list.append(jbt.dsp_jabatan)
        jabatan_nama_concat = "'" + "','".join(jabatan_nama_list) + "'"
        jabatan_subsatuankerja = db.session.execute(
            text(
                f"""
               select
                   a.struktur_id,
                   trim(lower(a.jabatan_nama)) as jabatan_nama,
                   trim(lower(a.jabatan_nama_panjang)) jabatan_nama_panjang,
                   a.jumlah_perwira,
                   a.jumlah_bintara,
                   a.jumlah_tamtama,
                   a.jumlah_pns,
                   a.jumlah_total,
                   b.parent_id,
                   b.subsatuankerja_id
               from raw.raw_m_jabatan a
               join raw.raw_s_subsatuankerja b on b.subsatuankerja_id = a.struktur_id
               where a.jabatan_status = 1
               and trim(lower(a.jabatan_nama)) in ({jabatan_nama_concat})
               and b.satuankerja_id = '{form_data.get("satuankerja_id")}'
               and b.subsatuankerja_status = 1
               order by b.parent_id asc
               """
            )
        )
        jab_subsat = jabatan_subsatuankerja.fetchall()

        jabatan_satuankerja = db.session.execute(
            text(
                f"""
                select                                                          
                    a.struktur_id,                                              
                    trim(lower(a.jabatan_nama)) as jabatan_nama,                
                    trim(lower(a.jabatan_nama_panjang)) as jabatan_nama_panjang,
                    a.jumlah_perwira,                                           
                    a.jumlah_bintara,                                           
                    a.jumlah_tamtama,                                           
                    a.jumlah_pns,                                               
                    a.jumlah_total                                              
                from raw.raw_m_jabatan  a                                       
                where a.jabatan_status=1                                        
                and trim(lower(a.jabatan_nama)) in ({jabatan_nama_concat})           
                and a.struktur_id = '{form_data.get("satuankerja_id")}'                  
                """
            )
        )
        jab_sat = jabatan_satuankerja.fetchall()
        raw_dsp_data = db.session.execute(raw_dsp_stmt)
        for dsp in raw_dsp_data.scalars():
            dsp_jabatan = dsp.__dict__
            del dsp_jabatan["_sa_instance_state"]
            if "created_at" in dsp_jabatan.keys():
                del dsp_jabatan["created_at"]
            if "updated_at" in dsp_jabatan.keys():
                del dsp_jabatan["updated_at"]
            if "id" in dsp_jabatan.keys():
                del dsp_jabatan["id"]
            jab_exists = False
            if dsp_jabatan["dsp_nomor"] is not None:
                for jab in jab_subsat:
                    if jab[1] == dsp_jabatan["dsp_jabatan"]:
                        jab_exists = True
                        parent_id = jab[8]
                        if jab[8] == "":
                            parent_node = dsp_jabatan_list[0]
                            parent_id = parent_node["sisfopers_struktur_id"]

                        dsp_jabatan["sisfopers_struktur_id"] = jab[0]
                        dsp_jabatan["sisfopers_jabatan_nama_panjang"] = jab[2]
                        dsp_jabatan["sisfopers_jumlah_perwira"] = jab[3]
                        dsp_jabatan["sisfopers_jumlah_bintara"] = jab[4]
                        dsp_jabatan["sisfopers_jumlah_tamtama"] = jab[5]
                        dsp_jabatan["sisfopers_jumlah_pns"] = jab[6]
                        dsp_jabatan["sisfopers_jumlah_total"] = jab[7]
                        dsp_jabatan["sisfopers_parent_id"] = parent_id
                        dsp_jabatan["sisfopers_subsatuankerja_id"] = jab[9]
                        dsp_jabatan["compare_status"] = 1
                        break
            else:
                for idx in range(len(dsp_jabatan_list) - 1, -1, -1):
                    parent = dsp_jabatan_list[idx]
                    if parent["dsp_nomor"] is not None:
                        if "sisfopers_subsatuankerja_id" in parent.keys():
                            dsp_jabatan["sisfopers_parent_id"] = parent[
                                "sisfopers_struktur_id"
                            ]
                            dsp_jabatan["sisfopers_parent_nomor"] = parent["dsp_nomor"]
                            dsp_jabatan["sisfopers_jumlah_perwira"] = 0
                            dsp_jabatan["sisfopers_jumlah_bintara"] = 0
                            dsp_jabatan["sisfopers_jumlah_tamtama"] = 0
                            dsp_jabatan["sisfopers_jumlah_pns"] = 0
                            dsp_jabatan["sisfopers_jumlah_total"] = 0
                            dsp_jabatan["compare_status"] = 0
                            for jab in jab_subsat:
                                if (
                                    jab[1] == dsp_jabatan["dsp_jabatan"]
                                    and jab[8] == dsp_jabatan["sisfopers_parent_id"]
                                ):
                                    dsp_jabatan["sisfopers_struktur_id"] = jab[0]
                                    dsp_jabatan["sisfopers_jabatan_nama_panjang"] = jab[
                                        2
                                    ]
                                    dsp_jabatan["sisfopers_jumlah_perwira"] = jab[3]
                                    dsp_jabatan["sisfopers_jumlah_bintara"] = jab[4]
                                    dsp_jabatan["sisfopers_jumlah_tamtama"] = jab[5]
                                    dsp_jabatan["sisfopers_jumlah_pns"] = jab[6]
                                    dsp_jabatan["sisfopers_jumlah_total"] = jab[7]
                                    dsp_jabatan["sisfopers_subsatuankerja_id"] = jab[9]
                                    dsp_jabatan["compare_status"] = 1
                                    break
                        else:
                            dsp_jabatan["sisfopers_parent_nomor"] = parent["dsp_nomor"]
                            dsp_jabatan["compare_status"] = 0
                            dsp_jabatan["sisfopers_jumlah_perwira"] = 0
                            dsp_jabatan["sisfopers_jumlah_bintara"] = 0
                            dsp_jabatan["sisfopers_jumlah_tamtama"] = 0
                            dsp_jabatan["sisfopers_jumlah_pns"] = 0
                            dsp_jabatan["sisfopers_jumlah_total"] = 0
                        break
            if not jab_exists:
                for jab in jab_sat:
                    if jab[1] == dsp_jabatan["dsp_jabatan"]:
                        if dsp_jabatan["dsp_jabatan"] is not None:
                            dsp_jabatan["sisfopers_struktur_id"] = jab[0]
                            dsp_jabatan["sisfopers_jabatan_nama_panjang"] = jab[2]
                            dsp_jabatan["sisfopers_jumlah_perwira"] = jab[3]
                            dsp_jabatan["sisfopers_jumlah_bintara"] = jab[4]
                            dsp_jabatan["sisfopers_jumlah_tamtama"] = jab[5]
                            dsp_jabatan["sisfopers_jumlah_pns"] = jab[6]
                            dsp_jabatan["sisfopers_jumlah_total"] = jab[7]
                            dsp_jabatan["sisfopers_parent_id"] = ""
                            dsp_jabatan["sisfopers_subsatuankerja_id"] = ""
                            dsp_jabatan["compare_status"] = 1

            dsp_jabatan_list.append(dsp_jabatan)
            counter = counter + 1

        try:
            delete_stmt = delete(RawComparison).where(
                RawComparison.dsp_nomor_keputusan_kasau
                == form_data.get("nomor_keputusan_kasau")
            )
            db.session.execute(delete_stmt)
            db.session.execute(insert(RawComparison), dsp_jabatan_list)
            db.session.commit()
        except Exception as ex:
            print(f"error insert bulk = {ex}")
        resp = {
            "status": 200,
            "message": "dsp files uploaded successfully",
            "dsp_list": dsp_jabatan_list,
        }
        return resp

    return app
