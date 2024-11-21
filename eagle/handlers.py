"""
Request handlers
"""

import os

import numpy as np
import pandas as pd
from flask import Blueprint, current_app, request
from sqlalchemy import CursorResult, delete, text
from sqlalchemy.sql import insert
from werkzeug.utils import secure_filename

from eagle.models.raw_comparison import RawComparison
from eagle.models.raw_dsp import RawDsp
from extensions import db

bp = Blueprint("handlers", __name__)


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


def check_kotama_exists(kotama_id, kotama_nama):
    resp = None
    if kotama_id is None or not kotama_id:
        resp = {"status": 400, "message": "kotama_id should not be empty"}
    check_kotama_id_stmt = db.session.execute(
        text(f"select count(1) from raw.raw_s_kotama where kotama_id='{kotama_id}'")
    )
    kotama_id_exists = check_kotama_id_stmt.one()
    if kotama_id_exists[0] != 1:
        resp = {"status": 404, "message": "kotama_id does not exists"}
        return resp, 404
    if kotama_nama is None or not kotama_nama:
        resp = {"status": 400, "message": "kotama nama should not be empty"}
        return resp, 400
    return resp


def check_satuankerja_exists(satuankerja_id, satuankerja_nama, kotama_id):
    resp = None
    if satuankerja_id is None or not satuankerja_id:
        resp = {"status": 400, "message": "satuankerja_id should not be empty"}
        return resp, 400
    check_satuankerja_id_stmt = db.session.execute(
        text(
            f"select count(1) from raw.raw_s_satuankerja where satuankerja_id='{satuankerja_id}' and kotama_id='{kotama_id}'"
        )
    )
    satuankerja_id_exists = check_satuankerja_id_stmt.one()
    if satuankerja_id_exists[0] != 1:
        resp = {"status": 404, "message": "satuankerja_id does not exists"}
        return resp, 404
    if satuankerja_nama is None or not satuankerja_nama:
        resp = {"status": 400, "message": "satuankerja_nama should not be empty"}
        return resp, 400
    return resp


def delete_raw_dsp_by_kepkasau_and_satuankerja_id(kepkasau, satuankerja_id, file_name):
    delete_raw_dsp = (
        delete(RawDsp)
        .where(RawDsp.dsp_nomor_keputusan_kasau == kepkasau)
        .where(RawDsp.dsp_satuankerja_id == satuankerja_id)
        .where(RawDsp.file_name == file_name)
    )
    db.session.execute(delete_raw_dsp)
    db.session.commit()


def delete_raw_comparison_by_kepkasau_and_satuankerja_id(
    kepkasau, satuankerja_id, file_name
):
    delete_comparison_dsp = (
        delete(RawComparison)
        .where(RawComparison.dsp_nomor_keputusan_kasau == kepkasau)
        .where(RawComparison.dsp_satuankerja_id == satuankerja_id)
        .where(RawComparison.file_name == file_name)
    )
    db.session.execute(delete_comparison_dsp)
    db.session.commit()


def jabatan_satker(satuankerja_id) -> CursorResult[any]:
    query = f"""
        select * from (
        select struktur_id, 'SAT' as satker_type, b.satuankerja_nama as satker_nama, '' as parent_id, 
        	trim(lower(jabatan_nama)) as jabatan_nama, jabatan_nama_panjang, jumlah_perwira, jumlah_bintara, jumlah_tamtama, jumlah_pns, jumlah_total 
        from raw.raw_m_jabatan a 
        left join raw.raw_s_satuankerja b on b.satuankerja_id = a.struktur_id 
        where b.satuankerja_id = '{satuankerja_id}' and a.jabatan_status=1 and b.satuankerja_status = 1
        union
        select struktur_id, 'SUB' as satker_type, b.subsatuankerja_nama as satker_nama, 
            case 
                when b.parent_id = '' then b.satuankerja_id 
                else b.parent_id
            end 
            as parent_id, 
        	trim(lower(jabatan_nama)) as jabatan_nama, jabatan_nama_panjang, jumlah_perwira, jumlah_bintara, jumlah_tamtama, jumlah_pns, jumlah_total 
        from raw.raw_m_jabatan a 
        left join raw.raw_s_subsatuankerja b on b.subsatuankerja_id = a.struktur_id 
        where b.satuankerja_id = '{satuankerja_id}' and b.subsatuankerja_status = 1 and a.jabatan_status = 1 
        ) as jab order by jab.parent_id asc
        """
    with db.engine.connect() as con:
        result = con.execute(text(query))

    return result


def get_child_struktur_id(parent_id: str, jab_satkers, jab_satker_filtered) -> str:
    """
    lookup child
    """
    struktur_id = ""
    for jsf in jab_satker_filtered:
        jab_satker_parent_id = jsf[3]
        if parent_id == jab_satker_parent_id:
            struktur_id = jsf[0]
        else:
            for js in jab_satkers:
                if js[0] == jab_satker_parent_id and js[3] == parent_id:
                    struktur_id = jsf[0]
    return struktur_id


def get_child_struktur_id_mabes(jab_satker_filtered, dsp_list) -> str:
    for jsf in jab_satker_filtered:
        jab_satker_parent_id = jsf[3]
        parent_dsp = [
            x
            for x in dsp_list
            if x["compare_status"] != 0
            and jab_satker_parent_id == x["sisfopers_struktur_id"]
        ]
        if len(parent_dsp) > 0:
            return jsf[0]
    return ""


def set_raw_dsp(raw_dsp: dict, filtered_jab):
    """
    set raw_dsp
    """
    raw_dsp["sisfopers_struktur_id"] = filtered_jab[0]
    raw_dsp["sisfopers_jabatan_nama_panjang"] = filtered_jab[5].strip()
    raw_dsp["sisfopers_jumlah_perwira"] = float(filtered_jab[6])
    raw_dsp["sisfopers_jumlah_bintara"] = float(filtered_jab[7])
    raw_dsp["sisfopers_jumlah_tamtama"] = float(filtered_jab[8])
    raw_dsp["sisfopers_jumlah_pns"] = float(filtered_jab[9])
    raw_dsp["sisfopers_jumlah_total"] = float(filtered_jab[10])
    raw_dsp["sisfopers_parent_id"] = filtered_jab[3]
    raw_dsp["sisfopers_subsatuankerja_id"] = filtered_jab[0]
    raw_dsp["compare_status"] = 1


@bp.route("/preview-dsp", methods=["POST"])
def preview_dsp():
    """
    Preview dsp handler
    """
    if "dsp_file" not in request.files:
        resp = {"status": 400, "message": "dsp_file should not be empty"}
        return resp, 400
    dsp_file = request.files["dsp_file"]
    filename = dsp_file.filename
    if not filename:
        resp = {"status": 400, "message": "no selected file"}
        return resp, 400
    form_data = request.form.to_dict()
    if form_data.get("nomor_keputusan_kasau") is None or not form_data.get(
        "nomor_keputusan_kasau"
    ):
        resp = {
            "status": 400,
            "message": "nomor_keputusan_kasau should not to be empty",
        }
        return resp

    check_kotama_resp = check_kotama_exists(
        form_data.get("kotama_id"), form_data.get("kotama_nama")
    )
    if check_kotama_resp is not None:
        return check_kotama_resp

    check_satuankerja_resp = check_satuankerja_exists(
        form_data.get("satuankerja_id"),
        form_data.get("satuankerja_nama"),
        form_data.get("kotama_id"),
    )
    if check_satuankerja_resp is not None:
        return check_satuankerja_resp

    dsp_file_path = os.path.join(
        current_app.config["UPLOAD_FOLDER"], secure_filename(filename)
    )
    dsp_file.save(dsp_file_path)
    df_dsp = read_dsp_file(dsp_file_path)
    df_dsp["dsp_satuankerja_id"] = form_data.get("satuankerja_id")
    df_dsp["dsp_satuankerja_nama"] = form_data.get("satuankerja_nama")
    df_dsp["dsp_nomor_keputusan_kasau"] = form_data.get("nomor_keputusan_kasau")
    df_dsp["file_name"] = filename
    df_dsp["dsp_subsatuankerja_nama"] = form_data.get("dsp_subsatuankerja_nama")
    df_dsp["dsp_subsatuankerja_id"] = form_data.get("dsp_subsatuankerja_id")
    df_dsp["dsp_subsatparent_nama"] = form_data.get("dsp_subsatparent_nama")
    df_dsp["dsp_subsatparent_id"] = form_data.get("dsp_subsatparent_id")
    df_dsp["dsp_subsatuankerja_level1_id"] = form_data.get(
        "dsp_subsatuankerja_level1_id"
    )
    df_dsp["dsp_subsatuankerja_level1_nama"] = form_data.get(
        "dsp_subsatuankerja_level1_nama"
    )
    df_dsp["kotama_id"] = form_data.get("kotama_id")
    df_dsp["kotama_nama"] = form_data.get("kotama_nama")

    df_jabatan_satker = jabatan_satker(form_data.get("satuankerja_id"))
    jab_satkers = list(df_jabatan_satker)

    raw_dsp_list = []
    for row in df_dsp.itertuples(index=True, name="Dsp"):
        dsp_gol_jab = (
            ""
            if isinstance(getattr(row, "dsp_gol_jab"), float)
            else getattr(row, "dsp_gol_jab")
        )
        dsp_ket = (
            ""
            if isinstance(getattr(row, "dsp_ket"), float)
            else getattr(row, "dsp_ket")
        )
        dsp_nomor = (
            ""
            if isinstance(getattr(row, "dsp_nomor"), float)
            else getattr(row, "dsp_nomor")
        )
        raw_dsp = {
            "dsp_ba": float(np.nan_to_num(getattr(row, "dsp_ba"))),
            "dsp_bidang_profesi": getattr(row, "dsp_bidang_profesi"),
            "dsp_gol_jab": dsp_gol_jab,
            "dsp_jml": float(np.nan_to_num(getattr(row, "dsp_jml"))),
            "dsp_ket": dsp_ket,
            "dsp_korps": getattr(row, "dsp_korps"),
            "dsp_nomor": dsp_nomor,
            "dsp_nomor_keputusan_kasau": getattr(row, "dsp_nomor_keputusan_kasau"),
            "dsp_pa": float(np.nan_to_num(getattr(row, "dsp_pa"))),
            "dsp_pangkat": getattr(row, "dsp_pangkat"),
            "dsp_pns": float(np.nan_to_num(getattr(row, "dsp_pns"))),
            "dsp_satuankerja_id": getattr(row, "dsp_satuankerja_id"),
            "dsp_satuankerja_nama": getattr(row, "dsp_satuankerja_nama"),
            "dsp_spesialisasi": getattr(row, "dsp_spesialisasi"),
            "dsp_subsatparent_id": getattr(row, "dsp_subsatparent_id"),
            "dsp_subsatparent_nama": getattr(row, "dsp_subsatparent_nama"),
            "dsp_subsatuankerja_id": getattr(row, "dsp_subsatuankerja_id"),
            "dsp_subsatuankerja_level1_id": getattr(
                row, "dsp_subsatuankerja_level1_id"
            ),
            "dsp_subsatuankerja_level1_nama": getattr(
                row, "dsp_subsatuankerja_level1_nama"
            ),
            "dsp_subsatuankerja_nama": getattr(row, "dsp_subsatuankerja_nama"),
            "dsp_ta": float(np.nan_to_num(getattr(row, "dsp_ta"))),
            "file_name": getattr(row, "file_name"),
            "dsp_jabatan": getattr(row, "dsp_jabatan"),
            "index": getattr(row, "Index"),
            "kotama_id": getattr(row, "kotama_id"),
            "kotama_nama": getattr(row, "kotama_nama"),
            "compare_status": 0,
        }

        jab_filtered = [x for x in jab_satkers if raw_dsp["dsp_jabatan"] in x[4]]

        if len(jab_filtered) > 0:
            if (
                raw_dsp["dsp_satuankerja_id"]
                and not raw_dsp["dsp_subsatparent_id"]
                and not raw_dsp["dsp_subsatuankerja_id"]
                and not raw_dsp["dsp_subsatuankerja_level1_id"]
            ):
                jab_satker = [
                    x for x in jab_filtered if raw_dsp["dsp_satuankerja_id"] == x[0]
                ]
                if len(jab_satker) > 0:
                    set_raw_dsp(raw_dsp, jab_satker[0])
                else:
                    struktur_id = get_child_struktur_id(
                        raw_dsp["dsp_satuankerja_id"],
                        jab_satkers,
                        jab_filtered,
                    )
                    jab_flt = [x for x in jab_filtered if x[0] == struktur_id]
                    if len(jab_flt) > 0:
                        set_raw_dsp(raw_dsp, jab_flt[0])
                    else:
                        struktur_id = get_child_struktur_id_mabes(
                            jab_filtered, raw_dsp_list
                        )
                        jab_flt = [x for x in jab_filtered if x[0] == struktur_id]
                        set_raw_dsp(raw_dsp, jab_flt[0])
            if (
                raw_dsp["dsp_satuankerja_id"]
                and raw_dsp["dsp_subsatparent_id"]
                and raw_dsp["dsp_subsatuankerja_id"]
                and not raw_dsp["dsp_subsatuankerja_level1_id"]
            ):
                jab_satker = []
                for jf in jab_filtered:
                    if jf[0] == raw_dsp["dsp_subsatuankerja_id"]:
                        jab_satker.append(jf)
                if len(jab_satker) > 0:
                    set_raw_dsp(raw_dsp, jab_satker[0])
                else:
                    struktur_id = get_child_struktur_id(
                        raw_dsp["dsp_subsatuankerja_id"],
                        jab_satkers,
                        jab_filtered,
                    )
                    jab_flt = []
                    for jf in jab_filtered:
                        if jf[0] == struktur_id:
                            jab_flt.append(jf)
                    if len(jab_flt) > 0:
                        set_raw_dsp(raw_dsp, jab_flt[0])
            if raw_dsp["dsp_subsatuankerja_level1_id"]:
                jab_level1 = []
                for jf in jab_filtered:
                    if jf[0] == raw_dsp["dsp_subsatuankerja_level1_id"]:
                        jab_level1.append(jf)
                if len(jab_level1) > 0:
                    set_raw_dsp(raw_dsp, jab_level1[0])
                else:
                    struktur_id = get_child_struktur_id(
                        raw_dsp["dsp_subsatuankerja_level1_id"],
                        jab_satkers,
                        jab_filtered,
                    )
                    jab_flt = []
                    for jf in jab_filtered:
                        if jf[0] == struktur_id:
                            jab_flt.append(jf)
                    if len(jab_flt) > 0:
                        set_raw_dsp(raw_dsp, jab_flt[0])
        raw_dsp_list.append(raw_dsp)

    def filter_not_paired(dsp):
        if dsp["compare_status"] == 0:
            return True
        return False

    def map_jabatan_nama(dsp):
        return dsp["dsp_jabatan"]

    filter_jabatan = filter(filter_not_paired, raw_dsp_list)
    list_not_paired = list(filter_jabatan)
    count_not_paired = len(list_not_paired)
    not_paired_jabatan = list(map(map_jabatan_nama, list_not_paired))
    resp = {
        "status": 200,
        "message": "dsp files uploaded successfully",
        "dsp_list": raw_dsp_list,
        "count_not_paired_jabatan": count_not_paired,
        "not_paired_jabatan": not_paired_jabatan,
    }
    return resp


@bp.route("/upload-dsp", methods=["POST"])
def upload_dsp():
    """
    Preview dsp handler
    """
    if "dsp_file" not in request.files:
        resp = {"status": 400, "message": "dsp_file should not be empty"}
        return resp, 400
    dsp_file = request.files["dsp_file"]
    filename = dsp_file.filename
    if not filename:
        resp = {"status": 400, "message": "no selected file"}
        return resp, 400
    form_data = request.form.to_dict()
    if form_data.get("nomor_keputusan_kasau") is None or not form_data.get(
        "nomor_keputusan_kasau"
    ):
        resp = {
            "status": 400,
            "message": "nomor_keputusan_kasau should not to be empty",
        }
        return resp

    check_kotama_resp = check_kotama_exists(
        form_data.get("kotama_id"), form_data.get("kotama_nama")
    )
    if check_kotama_resp is not None:
        return check_kotama_resp

    check_satuankerja_resp = check_satuankerja_exists(
        form_data.get("satuankerja_id"),
        form_data.get("satuankerja_nama"),
        form_data.get("kotama_id"),
    )
    if check_satuankerja_resp is not None:
        return check_satuankerja_resp

    dsp_file_path = os.path.join(
        current_app.config["UPLOAD_FOLDER"], secure_filename(filename)
    )

    delete_raw_dsp_by_kepkasau_and_satuankerja_id(
        form_data.get("nomor_keputusan_kasau"),
        form_data.get("satuankerja_id"),
        filename,
    )

    dsp_file.save(dsp_file_path)
    df_dsp = read_dsp_file(dsp_file_path)
    df_dsp["dsp_satuankerja_id"] = form_data.get("satuankerja_id")
    df_dsp["dsp_satuankerja_nama"] = form_data.get("satuankerja_nama")
    df_dsp["dsp_nomor_keputusan_kasau"] = form_data.get("nomor_keputusan_kasau")
    df_dsp["file_name"] = filename
    df_dsp["dsp_subsatuankerja_nama"] = form_data.get("dsp_subsatuankerja_nama")
    df_dsp["dsp_subsatuankerja_id"] = form_data.get("dsp_subsatuankerja_id")
    df_dsp["dsp_subsatparent_nama"] = form_data.get("dsp_subsatparent_nama")
    df_dsp["dsp_subsatparent_id"] = form_data.get("dsp_subsatparent_id")
    df_dsp["dsp_subsatuankerja_level1_id"] = form_data.get(
        "dsp_subsatuankerja_level1_id"
    )
    df_dsp["dsp_subsatuankerja_level1_nama"] = form_data.get(
        "dsp_subsatuankerja_level1_nama"
    )
    df_dsp["kotama_id"] = form_data.get("kotama_id")
    df_dsp["kotama_nama"] = form_data.get("kotama_nama")
    df_dsp.to_sql(
        name="raw_dsp",
        con=db.engine,
        index=False,
        if_exists="append",
        schema="raw",
    )
    df_dsp.reset_index()
    df_jabatan_satker = jabatan_satker(form_data.get("satuankerja_id"))
    jab_satkers = list(df_jabatan_satker)
    raw_dsp_list = []
    for row in df_dsp.itertuples(index=True, name="Dsp"):
        dsp_gol_jab = (
            ""
            if isinstance(getattr(row, "dsp_gol_jab"), float)
            else getattr(row, "dsp_gol_jab")
        )
        dsp_ket = (
            ""
            if isinstance(getattr(row, "dsp_ket"), float)
            else getattr(row, "dsp_ket")
        )
        dsp_nomor = (
            ""
            if isinstance(getattr(row, "dsp_nomor"), float)
            else getattr(row, "dsp_nomor")
        )
        raw_dsp = {
            "dsp_ba": float(np.nan_to_num(getattr(row, "dsp_ba"))),
            "dsp_bidang_profesi": getattr(row, "dsp_bidang_profesi"),
            "dsp_gol_jab": dsp_gol_jab,
            "dsp_jml": float(np.nan_to_num(getattr(row, "dsp_jml"))),
            "dsp_ket": dsp_ket,
            "dsp_korps": getattr(row, "dsp_korps"),
            "dsp_nomor": dsp_nomor,
            "dsp_nomor_keputusan_kasau": getattr(row, "dsp_nomor_keputusan_kasau"),
            "dsp_pa": float(np.nan_to_num(getattr(row, "dsp_pa"))),
            "dsp_pangkat": getattr(row, "dsp_pangkat"),
            "dsp_pns": float(np.nan_to_num(getattr(row, "dsp_pns"))),
            "dsp_satuankerja_id": getattr(row, "dsp_satuankerja_id"),
            "dsp_satuankerja_nama": getattr(row, "dsp_satuankerja_nama"),
            "dsp_spesialisasi": getattr(row, "dsp_spesialisasi"),
            "dsp_subsatparent_id": getattr(row, "dsp_subsatparent_id"),
            "dsp_subsatparent_nama": getattr(row, "dsp_subsatparent_nama"),
            "dsp_subsatuankerja_id": getattr(row, "dsp_subsatuankerja_id"),
            "dsp_subsatuankerja_level1_id": getattr(
                row, "dsp_subsatuankerja_level1_id"
            ),
            "dsp_subsatuankerja_level1_nama": getattr(
                row, "dsp_subsatuankerja_level1_nama"
            ),
            "dsp_subsatuankerja_nama": getattr(row, "dsp_subsatuankerja_nama"),
            "dsp_ta": float(np.nan_to_num(getattr(row, "dsp_ta"))),
            "file_name": getattr(row, "file_name"),
            "dsp_jabatan": getattr(row, "dsp_jabatan"),
            "index": getattr(row, "Index"),
            "kotama_id": getattr(row, "kotama_id"),
            "kotama_nama": getattr(row, "kotama_nama"),
            "compare_status": 0,
        }
        jab_filtered = [x for x in jab_satkers if raw_dsp["dsp_jabatan"] in x[4]]
        if len(jab_filtered) > 0:
            if (
                raw_dsp["dsp_satuankerja_id"]
                and not raw_dsp["dsp_subsatparent_id"]
                and not raw_dsp["dsp_subsatuankerja_id"]
                and not raw_dsp["dsp_subsatuankerja_level1_id"]
            ):
                jab_satker = [
                    x for x in jab_filtered if raw_dsp["dsp_satuankerja_id"] == x[0]
                ]
                if len(jab_satker) > 0:
                    set_raw_dsp(raw_dsp, jab_satker[0])
                else:
                    struktur_id = get_child_struktur_id(
                        raw_dsp["dsp_satuankerja_id"],
                        jab_satkers,
                        jab_filtered,
                    )
                    jab_flt = [x for x in jab_filtered if x[0] == struktur_id]
                    if len(jab_flt) > 0:
                        set_raw_dsp(raw_dsp, jab_flt[0])
                    else:
                        struktur_id = get_child_struktur_id_mabes(
                            jab_filtered, raw_dsp_list
                        )
                        jab_flt = [x for x in jab_filtered if x[0] == struktur_id]
                        set_raw_dsp(raw_dsp, jab_flt[0])
            if (
                raw_dsp["dsp_satuankerja_id"]
                and raw_dsp["dsp_subsatparent_id"]
                and raw_dsp["dsp_subsatuankerja_id"]
                and not raw_dsp["dsp_subsatuankerja_level1_id"]
            ):
                jab_satker = [
                    x for x in jab_filtered if x[0] == raw_dsp["dsp_subsatuankerja_id"]
                ]
                if len(jab_satker) > 0:
                    set_raw_dsp(raw_dsp, jab_satker[0])
                else:
                    struktur_id = get_child_struktur_id(
                        raw_dsp["dsp_subsatuankerja_id"],
                        jab_satkers,
                        jab_filtered,
                    )
                    jab_flt = [x for x in jab_filtered if x[0] == struktur_id]
                    if len(jab_flt) > 0:
                        set_raw_dsp(raw_dsp, jab_flt[0])
            if raw_dsp["dsp_subsatuankerja_level1_id"]:
                jab_level1 = [
                    x
                    for x in jab_filtered
                    if x[0] == raw_dsp["dsp_subsatuankerja_level1_id"]
                ]
                if len(jab_level1) > 0:
                    set_raw_dsp(raw_dsp, jab_level1[0])
                else:
                    struktur_id = get_child_struktur_id(
                        raw_dsp["dsp_subsatuankerja_level1_id"],
                        jab_satkers,
                        jab_filtered,
                    )
                    jab_flt = [x for x in jab_filtered if x[0] == struktur_id]
                    if len(jab_flt) > 0:
                        set_raw_dsp(raw_dsp, jab_flt[0])
        raw_dsp_list.append(raw_dsp)
    delete_raw_comparison_by_kepkasau_and_satuankerja_id(
        form_data.get("nomor_keputusan_kasau"),
        form_data.get("satuankerja_id"),
        filename,
    )
    db.session.execute(insert(RawComparison), raw_dsp_list)
    db.session.commit()
    resp = {
        "status": 200,
        "message": "dsp files uploaded successfully",
        "dsp_list": raw_dsp_list,
    }
    return resp
