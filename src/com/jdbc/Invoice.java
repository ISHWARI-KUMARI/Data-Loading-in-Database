//Plain Old Java Object(POJO class)

package com.jdbc;

import java.sql.Date;
public class Invoice {
  public Invoice(String businessCode, String custNumber, String nameCustomer, Date clearDate, int businessYear,
      long docId, Date postingDate, Date documentCreateDate, Date dueInDate, String invoiceCurrency,
      String documentType, boolean postingId, String areaBusiness, Double totalOpenAmount,
      Date baselineCreateDate, String custPaymentTerms, long invoiceId, boolean isOpen) {
    super();
    this.businessCode = businessCode;
    this.custNumber = custNumber;
    this.nameCustomer = nameCustomer;
    this.clearDate = clearDate;
    this.businessYear = businessYear;
    this.docId = docId;
    this.postingDate = postingDate;
    this.documentCreateDate = documentCreateDate;
    this.dueInDate = dueInDate;
    this.invoiceCurrency = invoiceCurrency;
    this.documentType = documentType;
    this.postingId = postingId;
    this.areaBusiness = areaBusiness;
    this.totalOpenAmount = totalOpenAmount;
    this.baselineCreateDate = baselineCreateDate;
    this.custPaymentTerms = custPaymentTerms;
    this.invoiceId = invoiceId;
    this.isOpen = isOpen;
  }
  public Date getDocumentCreateDate() {
    return documentCreateDate;
  }
  public void setDocumentCreateDate(Date documentCreateDate) {
    this.documentCreateDate = documentCreateDate;
  }
  public Date getDueInDate() {
    return dueInDate;
  }
  public void setDueInDate(Date dueInDate) {
    this.dueInDate = dueInDate;
  }
  public String getBusinessCode() {
    return businessCode;
  }
  public void setBusinessCode(String businessCode) {
    this.businessCode = businessCode;
  }
  public String getCustNumber() {
    return custNumber;
  }
  public void setCustNumber(String custNumber) {
    this.custNumber = custNumber;
  }
  public String getNameCustomer() {
    return nameCustomer;
  }
  public void setNameCustomer(String nameCustomer) {
    this.nameCustomer = nameCustomer;
  }
  public Date getClearDate() {
    return clearDate;
  }
  public void setClearDate(Date clearDate) {
    this.clearDate = clearDate;
  }
  public int getBusinessYear() {
    return businessYear;
  }
  public void setBusinessYear(int businessYear) {
    this.businessYear = businessYear;
  }
  public long getDocId() {
    return docId;
  }
  public void setDocId(long docId) {
    this.docId = docId;
  }
  public Date getPostingDate() {
    return postingDate;
  }
  public void setPostingDate(Date postingDate) {
    this.postingDate = postingDate;
  }
  public String getInvoiceCurrency() {
    return invoiceCurrency;
  }
  public void setInvoiceCurrency(String invoiceCurrency) {
    this.invoiceCurrency = invoiceCurrency;
  }
  public String getDocumentType() {
    return documentType;
  }
  public void setDocumentType(String documentType) {
    this.documentType = documentType;
  }
  public boolean isPostingId() {
    return postingId;
  }
  public void setPostingId(boolean postingId) {
    this.postingId = postingId;
  }
  public String getAreaBusiness() {
    return areaBusiness;
  }
  public void setAreaBusiness(String areaBusiness) {
    this.areaBusiness = areaBusiness;
  }
  public Double getTotalOpenAmount() {
    return totalOpenAmount;
  }
  public void setTotalOpenAmount(Double totalOpenAmount) {
    this.totalOpenAmount = totalOpenAmount;
  }
  public Date getBaselineCreateDate() {
    return baselineCreateDate;
  }
  public void setBaselineCreateDate(Date baselineCreateDate) {
    this.baselineCreateDate = baselineCreateDate;
  }
  public String getCustPaymentTerms() {
    return custPaymentTerms;
  }
  public void setCustPaymentTerms(String custPaymentTerms) {
    this.custPaymentTerms = custPaymentTerms;
  }
  public long getInvoiceId() {
    return invoiceId;
  }
  public void setInvoiceId(long invoiceId) {
    this.invoiceId = invoiceId;
  }
  public boolean isOpen() {
    return isOpen;
  }
  public void setOpen(boolean isOpen) {
    this.isOpen = isOpen;
  }
  String businessCode;
  String custNumber;
  String nameCustomer;
  Date clearDate;
  int businessYear;
  long docId;


  Date postingDate;
  Date documentCreateDate;
  Date dueInDate;
  String invoiceCurrency;
  String documentType;
  boolean postingId;
  String areaBusiness;
  Double totalOpenAmount;
  Date baselineCreateDate;
  String custPaymentTerms;
  long invoiceId;
  boolean isOpen;

  @Override
  public String toString() {
    return "Invoice [businessCode=" + businessCode + ", custNumber=" + custNumber + ", nameCustomer=" + nameCustomer
        + ", clearDate=" + clearDate + ", businessYear=" + businessYear + ", docId=" + docId + ", postingDate="
        + postingDate + ", documentCreateDate=" + documentCreateDate + ", dueInDate=" + dueInDate
        + ", invoiceCurrency=" + invoiceCurrency + ", documentType=" + documentType + ", postingId=" + postingId
        + ", areaBusiness=" + areaBusiness + ", totalOpenAmount=" + totalOpenAmount + ", baselineCreateDate="
        + baselineCreateDate + ", custPaymentTerms=" + custPaymentTerms + ", invoiceId=" + invoiceId
        + ", isOpen=" + isOpen + "]";
  }
}
