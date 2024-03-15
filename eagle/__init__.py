import os
from flask import Flask, request, Response
from sqlalchemy import select, delete, text, asc, insert
from config import Config
from eagle.extensions import db
from eagle.models.raw_dsp import RawDsp
from werkzeug.utils import secure_filename
from eagle.models.raw_comparison import RawComparison
import pandas as pd
import numpy as np


def read_dsp_file(dspfile) -> pd.DataFrame:
    df = pd.read_excel(dspfile, usecols="A:M", header=None)
    df = df.rename(
        columns={
            0: "dsp_nomor",
            1: "dsp_jabatan",
            2: "dsp_gol_jab",
            3: "dsp_pangkat",
            4: "dsp_korps",
            5: "dsp_bidang_profesi",
            6: "dsp_spesialisasi",
            7: "dsp_pa",
            8: "dsp_ba",
            9: "dsp_ta",
            10: "dsp_pns",
            11: "dsp_jml",
            12: "dsp_ket",
        }
    )
    df = df.dropna(how="all", subset=["dsp_jabatan"])
    df = df.replace(r" ", np.nan)
    df["dsp_jabatan"] = df["dsp_jabatan"].str.strip().str.lower()
    df["dsp_jabatan"] = df["dsp_jabatan"].replace("\n", " ", regex=True)
    df["dsp_pangkat"] = df["dsp_pangkat"].replace("\n", " ", regex=True)
    df["dsp_korps"] = df["dsp_korps"].replace("\n", " ", regex=True)
    df["dsp_bidang_profesi"] = df["dsp_bidang_profesi"].replace("\n", " ", regex=True)
    df["dsp_spesialisasi"] = df["dsp_spesialisasi"].replace("\n", " ", regex=True)
    df["dsp_ket"] = df["dsp_ket"].replace("\n", " ", regex=True)
    df = df.query("dsp_jabatan != '`'")
    df = df.query("dsp_jabatan != '2'")
    df = df.query("dsp_jabatan != 'J A B A T A N'")
    df = df.query("dsp_jabatan != 'JUMLAH'")
    df = df.query("dsp_jabatan != 'FUNGSIONAL'")
    df = df.query("dsp_jabatan != 'jumlah'")
    df = df.query("dsp_pa != 'P A N G K A T'")
    df = df.query("dsp_pa != 'P A N G K A T '")
    df = df.query("dsp_pa != 'PANGKAT'")
    df = df.query("dsp_pa != '         PANGKAT'")
    df = df.query("dsp_ta != '          '")
    df = df.query("dsp_ba != 'P A N G K A T'")
    df = df.query("dsp_ba != 'P A N G K A T '")
    df.astype(
        {
            "dsp_pa": float,
            "dsp_ba": float,
            "dsp_ta": float,
            "dsp_pns": float,
            "dsp_jml": float,
        }
    )
    return df


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
            return resp, 400
        dsp_file = request.files["dsp_file"]
        filename = dsp_file.filename
        if not filename:
            resp = {"status": 400, "message": "no selected file"}
            return resp, 400

        form_data = request.form.to_dict()
        if form_data.get("satuankerja_id") is None or not (
            form_data.get("satuankerja_id")
        ):
            resp = {"status": 400, "message": "satuankerja_id should not be empty"}
            return resp, 400
        if form_data.get("satuankerja_nama") is None or not (
            form_data.get("satuankerja_nama")
        ):
            resp = {"status": 400, "message": "satuankerja_nama should not be empty"}
            return resp, 400
        if form_data.get("nomor_keputusan_kasau") is None or not (
            form_data.get("nomor_keputusan_kasau")
        ):
            resp = {
                "status": 400,
                "message": "nmmor_keputusan_kasau should not be empty",
            }
            return resp, 400

        check_satuankerja_stmt = db.session.execute(
            text(
                f"select count(1) from raw.raw_s_satuankerja where satuankerja_id ='{form_data.get('satuankerja_id')}'"
            )
        )
        satuankerja_exists = check_satuankerja_stmt.one()
        if satuankerja_exists[0] != 1:
            resp = {
                "status": 404,
                "message": "satuankerja_id does not exists",
            }
            return resp, 404

        delete_stmt = (
            delete(RawDsp)
            .where(
                RawDsp.dsp_nomor_keputusan_kasau
                == form_data.get("nomor_keputusan_kasau")
            )
            .where(RawDsp.dsp_satuankerja_id == form_data.get("satuankerja_id"))
        )
        db.session.execute(delete_stmt)
        db.session.commit()

        dsp_file.save(
            os.path.join(app.config["UPLOAD_FOLDER"], secure_filename(filename))
        )

        dsp_file_path = app.config["UPLOAD_FOLDER"] + secure_filename(filename)
        df = read_dsp_file(dsp_file_path)
        df["dsp_satuankerja_id"] = form_data.get("satuankerja_id")
        df["dsp_satuankerja_nama"] = form_data.get("satuankerja_nama")
        df["dsp_nomor_keputusan_kasau"] = form_data.get("nomor_keputusan_kasau")

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
            .where(RawDsp.dsp_jabatan != None)
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
                and trim(lower(a.jabatan_nama)) in ({jabatan_nama_concat.lower()})
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
            subsatker_exists = dsp_jabatan["dsp_jabatan"] in [
                subsat[1] for subsat in jab_subsat
            ]
            parent = None
            ####cari parent node
            if dsp_jabatan["dsp_nomor"] is not None:
                nomor_node = dsp_jabatan["dsp_nomor"].rstrip(".")
                is_child_node = "." in nomor_node
                if not is_child_node:
                    if len(dsp_jabatan_list) != 0:
                        parent = dsp_jabatan_list[0]
                else:
                    parent_nomor = nomor_node[: nomor_node.rindex(".")]
                    start = len(dsp_jabatan_list) - 1
                    for idx in range(start, -1, -1):
                        if dsp_jabatan_list[idx]["dsp_nomor"] is not None:
                            dsp_parent = dsp_jabatan_list[idx]["dsp_nomor"].rstrip(".")
                            if parent_nomor == dsp_parent:
                                parent = dsp_jabatan_list[idx]
            else:
                start = len(dsp_jabatan_list) - 1
                for idx in range(start, -1, -1):
                    if dsp_jabatan_list[idx]["dsp_nomor"] is not None:
                        parent = dsp_jabatan_list[idx]
                        break

            if parent is None:
                exists_in_jabatan = dsp_jabatan["dsp_jabatan"] in [
                    jab[1] for jab in jab_sat if jab[1] == dsp_jabatan["dsp_jabatan"]
                ]
                exists_in_subsat = dsp_jabatan["dsp_jabatan"] in [
                    jab[1] for jab in jab_subsat if jab[1] == dsp_jabatan["dsp_jabatan"]
                ]
                if exists_in_jabatan:
                    for jab in jab_sat:
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
                elif exists_in_subsat:
                    for jab in jab_subsat:
                        if jab[1] == dsp_jabatan["dsp_jabatan"]:
                            dsp_jabatan["sisfopers_struktur_id"] = jab[0]
                            dsp_jabatan["sisfopers_jabatan_nama_panjang"] = jab[2]
                            dsp_jabatan["sisfopers_jumlah_perwira"] = jab[3]
                            dsp_jabatan["sisfopers_jumlah_bintara"] = jab[4]
                            dsp_jabatan["sisfopers_jumlah_tamtama"] = jab[5]
                            dsp_jabatan["sisfopers_jumlah_pns"] = jab[6]
                            dsp_jabatan["sisfopers_jumlah_total"] = jab[7]
                            dsp_jabatan["sisfopers_parent_id"] = jab[8] 
                            dsp_jabatan["sisfopers_subsatuankerja_id"] = jab[9] 
                            dsp_jabatan["compare_status"] = 1
                            break

                else:
                    dsp_jabatan["sisfopers_struktur_id"] = ""
                    dsp_jabatan["sisfopers_jabatan_nama_panjang"] = ""
                    dsp_jabatan["sisfopers_jumlah_perwira"] = None
                    dsp_jabatan["sisfopers_jumlah_bintara"] = None
                    dsp_jabatan["sisfopers_jumlah_tamtama"] = None
                    dsp_jabatan["sisfopers_jumlah_pns"] = None
                    dsp_jabatan["sisfopers_jumlah_total"] = None
                    dsp_jabatan["sisfopers_parent_id"] = ""
                    dsp_jabatan["sisfopers_subsatuankerja_id"] = ""
                    dsp_jabatan["compare_status"] = 0

            else:
                if subsatker_exists:
                    dsp_subsat = [
                        dsp_insubsat
                        for dsp_insubsat in jab_subsat
                        if dsp_insubsat[1] == dsp_jabatan["dsp_jabatan"]
                    ]
                    for subsat in dsp_subsat:
                        if (
                            dsp_jabatan["dsp_jabatan"] == subsat[1]
                            and subsat[8] == parent["sisfopers_subsatuankerja_id"]
                        ):
                            dsp_jabatan["sisfopers_struktur_id"] = subsat[0]
                            dsp_jabatan["sisfopers_jabatan_nama_panjang"] = subsat[2]
                            dsp_jabatan["sisfopers_jumlah_perwira"] = subsat[3]
                            dsp_jabatan["sisfopers_jumlah_bintara"] = subsat[4]
                            dsp_jabatan["sisfopers_jumlah_tamtama"] = subsat[5]
                            dsp_jabatan["sisfopers_jumlah_pns"] = subsat[6]
                            dsp_jabatan["sisfopers_jumlah_total"] = subsat[7]
                            dsp_jabatan["sisfopers_parent_id"] = parent[
                                "sisfopers_subsatuankerja_id"
                            ]
                            dsp_jabatan["sisfopers_subsatuankerja_id"] = subsat[9]
                            dsp_jabatan["sisfopers_parent_nomor"] = parent["dsp_nomor"]
                            dsp_jabatan["compare_status"] = 1
                            break
                        else:
                            dsp_jabatan["sisfopers_struktur_id"] = ""
                            dsp_jabatan["sisfopers_jabatan_nama_panjang"] = ""
                            dsp_jabatan["sisfopers_jumlah_perwira"] = None
                            dsp_jabatan["sisfopers_jumlah_bintara"] = None
                            dsp_jabatan["sisfopers_jumlah_tamtama"] = None
                            dsp_jabatan["sisfopers_jumlah_pns"] = None
                            dsp_jabatan["sisfopers_jumlah_total"] = None
                            dsp_jabatan["sisfopers_parent_id"] = ""
                            dsp_jabatan["sisfopers_subsatuankerja_id"] = ""
                            dsp_jabatan["sisfopers_parent_nomor"] = parent["dsp_nomor"]
                            dsp_jabatan["compare_status"] = 0

                else:
                    dsp_jabatan["sisfopers_struktur_id"] = ""
                    dsp_jabatan["sisfopers_jabatan_nama_panjang"] = ""
                    dsp_jabatan["sisfopers_jumlah_perwira"] = None
                    dsp_jabatan["sisfopers_jumlah_bintara"] = None
                    dsp_jabatan["sisfopers_jumlah_tamtama"] = None
                    dsp_jabatan["sisfopers_jumlah_pns"] = None
                    dsp_jabatan["sisfopers_jumlah_total"] = None
                    dsp_jabatan["sisfopers_parent_id"] = ""
                    dsp_jabatan["sisfopers_subsatuankerja_id"] = ""
                    dsp_jabatan["sisfopers_parent_nomor"] = parent["dsp_nomor"]
                    dsp_jabatan["compare_status"] = 0

            dsp_jabatan_list.append(dsp_jabatan)
            counter = counter + 1

        try:
            delete_stmt = (
                delete(RawComparison)
                .where(
                    RawComparison.dsp_nomor_keputusan_kasau
                    == form_data.get("nomor_keputusan_kasau")
                )
                .where(
                    RawComparison.dsp_satuankerja_id == form_data.get("satuankerja_id")
                )
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
