//
// Created by Administrator on 2024/9/14.
//
#include <com_open_crossdb_jni_core_CrossDBJNI.h>
#include "crossdb.h"

JNIEXPORT jlong JNICALL Java_com_open_crossdb_jni_core_CrossDBJNI_jniOpenDB
        (JNIEnv *env, jclass clazz, jstring path){
    const char *c_tpath = env->GetStringUTFChars(path, NULL);
    xdb_conn_t	*pConn = xdb_open (c_tpath);
    XDB_CHECK (NULL != pConn, printf ("failed to create DB\n"); return -1;)
    env->ReleaseStringUTFChars(path, c_tpath);
    return reinterpret_cast<jlong>(pConn);
}


JNIEXPORT jint JNICALL Java_com_open_crossdb_jni_core_CrossDBJNI_jniBegin
        (JNIEnv *env, jclass clazz, jlong conn){
    xdb_conn_t	*pConn = reinterpret_cast<xdb_conn_t *>(conn);
    xdb_begin(pConn);
    return 1;
}


JNIEXPORT jint JNICALL Java_com_open_crossdb_jni_core_CrossDBJNI_jniCommit
        (JNIEnv *env, jclass clazz, jlong conn){
    xdb_conn_t	*pConn = reinterpret_cast<xdb_conn_t *>(conn);
    xdb_commit(pConn);
    return 1;
}


JNIEXPORT jint JNICALL Java_com_open_crossdb_jni_core_CrossDBJNI_jniRollback
        (JNIEnv *env, jclass clazz, jlong conn){
    xdb_conn_t	*pConn = reinterpret_cast<xdb_conn_t *>(conn);
    xdb_rollback(pConn);
    return 1;
}


JNIEXPORT jlong JNICALL Java_com_open_crossdb_jni_core_CrossDBJNI_jniXdbExec
        (JNIEnv *env, jclass clazz, jlong conn, jstring sql){
    const char *c_sql = env->GetStringUTFChars(sql, NULL);
    xdb_conn_t	*pConn = reinterpret_cast<xdb_conn_t *>(conn);
    xdb_res_t	*pRes = xdb_exec (pConn, c_sql);
    env->ReleaseStringUTFChars(sql, c_sql);

    return reinterpret_cast<jlong>(pRes);
}


JNIEXPORT jlong JNICALL Java_com_open_crossdb_jni_core_CrossDBJNI_jniFetchRow
        (JNIEnv *env, jclass clazz,  jlong res){
    xdb_res_t	*pRes = reinterpret_cast<xdb_res_t *>(res);
    xdb_row_t	*pRow = xdb_fetch_row (pRes);
    return reinterpret_cast<jlong>(pRow);
}


JNIEXPORT jint JNICALL Java_com_open_crossdb_jni_core_CrossDBJNI_jniColumnInt
        (JNIEnv *env, jclass clazz,  jlong res, jlong row, jint colNum){
    xdb_res_t	*pRes = reinterpret_cast<xdb_res_t *>(res);
    xdb_row_t	*pRow = reinterpret_cast<xdb_row_t *>(row);
    return xdb_column_int (pRes->col_meta, pRow, colNum);
}


JNIEXPORT jstring JNICALL Java_com_open_crossdb_jni_core_CrossDBJNI_jniColumnStr
        (JNIEnv *env, jclass clazz,  jlong res, jlong row, jint colNum){
    xdb_res_t	*pRes = reinterpret_cast<xdb_res_t *>(res);
    xdb_row_t	*pRow = reinterpret_cast<xdb_row_t *>(row);
    return (env)->NewStringUTF(xdb_column_str(pRes->col_meta, pRow, colNum));
}


JNIEXPORT jfloat JNICALL Java_com_open_crossdb_jni_core_CrossDBJNI_jniColumnFloat
        (JNIEnv *env, jclass clazz,  jlong res, jlong row, jint colNum){
    xdb_res_t	*pRes = reinterpret_cast<xdb_res_t *>(res);
    xdb_row_t	*pRow = reinterpret_cast<xdb_row_t *>(row);
    return  xdb_column_float(pRes->col_meta, pRow, colNum);
}


JNIEXPORT jint JNICALL Java_com_open_crossdb_jni_core_CrossDBJNI_jniClose
        (JNIEnv *env, jclass clazz,  jlong conn){
    xdb_conn_t	*pConn = reinterpret_cast<xdb_conn_t *>(conn);
    xdb_close(pConn);
    return 1;
}
