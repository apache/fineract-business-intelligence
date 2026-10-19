/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.bi.ingestion.loadgen;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SourceWorkload {

    private static final Logger LOG = LoggerFactory.getLogger(SourceWorkload.class);
    private static final long ID_BASE = 10_000_000L;

    private SourceWorkload() {
    }

    static final class Db {
        final String name;
        final Connection conn;
        long nextClient;
        long nextLoan;
        long nextTxn;
        long nextSchedule;
        final List<Long> loans = new ArrayList<>();
        final List<Long> txns = new ArrayList<>();

        Db(String name, Connection conn) {
            this.name = name;
            this.conn = conn;
        }
    }

    public static void run(Args args) throws Exception {
        String template = args.get("jdbc-template", "jdbc:postgresql://localhost:5433/fineract_src_%03d");
        int sources = args.getInt("sources", 3);
        String user = args.get("db-user", "root");
        String password = args.get("db-password", "");
        double tps = args.getDouble("tps", 50);
        long durationSec = args.getLong("duration-sec", 60);
        int seedLoans = args.getInt("seed-loans", 200);
        long seed = args.getLong("seed", 7);
        SplittableRandom rnd = new SplittableRandom(seed);

        List<Db> dbs = new ArrayList<>();
        for (int i = 1; i <= sources; i++) {
            String url = String.format(template, i);
            Connection c = DriverManager.getConnection(url, user, password);
            c.setAutoCommit(false);
            Db db = new Db(url, c);
            prepare(db);
            for (int l = 0; l < seedLoans; l++) {
                newClientAndLoan(db, rnd);
            }
            db.conn.commit();
            dbs.add(db);
            LOG.info("Prepared {} ({} loans)", url, db.loans.size());
        }

        long started = System.nanoTime();
        long done = 0;
        long end = started + durationSec * 1_000_000_000L;
        while (System.nanoTime() < end) {
            Db db = dbs.get(rnd.nextInt(dbs.size()));
            double r = rnd.nextDouble();
            try {
                if (r < 0.60) {
                    repayment(db, rnd);
                } else if (r < 0.80) {
                    updateLoan(db, rnd);
                } else if (r < 0.92) {
                    newClientAndLoan(db, rnd);
                } else if (r < 0.98) {
                    reverse(db, rnd);
                } else {
                    deleteTransaction(db, rnd);
                }
                db.conn.commit();
                done++;
            } catch (SQLException e) {
                db.conn.rollback();
                LOG.warn("transaction failed on {}: {}", db.name, e.getMessage());
            }
            double expected = (System.nanoTime() - started) / 1e9 * tps;
            if (done > expected) {
                Thread.sleep((long) Math.max(1, (done - expected) / tps * 1000));
            }
        }
        double secs = (System.nanoTime() - started) / 1e9;
        LOG.info("Committed {} source transactions in {}s ({} tps) across {} databases",
                done, String.format("%.1f", secs), Math.round(done / secs), dbs.size());
        for (Db db : dbs) {
            db.conn.close();
        }
    }

    private static void prepare(Db db) throws SQLException {
        try (Statement st = db.conn.createStatement()) {
            st.execute("INSERT INTO m_office (id, parent_id, hierarchy, name, opening_date) "
                    + "VALUES (1, NULL, '.', 'Head Office', DATE '2015-01-01') ON CONFLICT (id) DO NOTHING");
            st.execute("INSERT INTO m_product_loan (id, short_name, currency_code, currency_digits, name, "
                    + "interest_method_enum, repay_every, repayment_period_frequency_enum, number_of_repayments, "
                    + "amortization_method_enum, accounting_type) VALUES (1, 'GL', 'INR', 2, 'Group Loan', 1, 1, 2, 12, "
                    + "1, 1) ON CONFLICT (id) DO NOTHING");
        }
        db.nextClient = maxId(db, "m_client") + 1;
        db.nextLoan = maxId(db, "m_loan") + 1;
        db.nextTxn = maxId(db, "m_loan_transaction") + 1;
        db.nextSchedule = maxId(db, "m_loan_repayment_schedule") + 1;
        try (Statement st = db.conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT id FROM m_loan WHERE id >= " + ID_BASE)) {
            while (rs.next()) {
                db.loans.add(rs.getLong(1));
            }
        }
        db.conn.commit();
    }

    private static long maxId(Db db, String table) throws SQLException {
        try (Statement st = db.conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COALESCE(MAX(id), 0) FROM " + table)) {
            rs.next();
            return Math.max(rs.getLong(1), ID_BASE);
        }
    }

    private static void newClientAndLoan(Db db, SplittableRandom rnd) throws SQLException {
        long client = db.nextClient++;
        long loan = db.nextLoan++;
        LocalDate disbursed = LocalDate.of(2025, 1, 1).plusDays(rnd.nextInt(500));
        long principal = 10_000 + rnd.nextInt(90_000);
        try (PreparedStatement ps = db.conn.prepareStatement(
                "INSERT INTO m_client (id, account_no, status_enum, office_id, display_name, activation_date, "
                        + "date_of_birth, created_on_utc, created_by, last_modified_by, last_modified_on_utc) "
                        + "VALUES (?, ?, 300, 1, ?, ?, ?, NOW(), 1, 1, NOW())")) {
            ps.setLong(1, client);
            ps.setString(2, "C" + client);
            ps.setString(3, "Client " + client);
            ps.setObject(4, disbursed);
            ps.setObject(5, LocalDate.of(1970, 1, 1).plusDays(rnd.nextInt(12_000)));
            ps.executeUpdate();
        }
        try (PreparedStatement ps = db.conn.prepareStatement(
                "INSERT INTO m_loan (id, account_no, client_id, product_id, loan_status_id, loan_type_enum, "
                        + "currency_code, currency_digits, principal_amount_proposed, principal_amount, approved_principal, "
                        + "net_disbursal_amount, interest_method_enum, repay_every, repayment_period_frequency_enum, "
                        + "number_of_repayments, amortization_method_enum, disbursedon_date, principal_disbursed_derived, "
                        + "principal_outstanding_derived, total_expected_repayment_derived, total_outstanding_derived, "
                        + "created_on_utc, created_by, last_modified_by, last_modified_on_utc) "
                        + "VALUES (?, ?, ?, 1, 300, 1, 'INR', 2, ?, ?, ?, ?, 1, 1, 2, 12, 1, ?, ?, ?, ?, ?, NOW(), 1, 1, NOW())")) {
            ps.setLong(1, loan);
            ps.setString(2, "L" + loan);
            ps.setLong(3, client);
            for (int i = 4; i <= 7; i++) {
                ps.setLong(i, principal);
            }
            ps.setObject(8, disbursed);
            ps.setLong(9, principal);
            ps.setLong(10, principal);
            ps.setLong(11, principal * 12 / 10);
            ps.setLong(12, principal * 12 / 10);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = db.conn.prepareStatement(
                "INSERT INTO m_loan_repayment_schedule (id, loan_id, fromdate, duedate, installment, principal_amount, "
                        + "interest_amount, completed_derived) VALUES (?, ?, ?, ?, ?, ?, ?, false)")) {
            for (int inst = 1; inst <= 12; inst++) {
                ps.setLong(1, db.nextSchedule++);
                ps.setLong(2, loan);
                ps.setObject(3, disbursed.plusMonths(inst - 1L));
                ps.setObject(4, disbursed.plusMonths(inst));
                ps.setInt(5, inst);
                ps.setLong(6, principal / 12);
                ps.setLong(7, principal / 50);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        insertTransaction(db, loan, 1, principal, disbursed);
        db.loans.add(loan);
    }

    private static void insertTransaction(Db db, long loan, int type, long amount, LocalDate date) throws SQLException {
        long id = db.nextTxn++;
        try (PreparedStatement ps = db.conn.prepareStatement(
                "INSERT INTO m_loan_transaction (id, loan_id, office_id, is_reversed, transaction_type_enum, "
                        + "transaction_date, amount, principal_portion_derived, interest_portion_derived, "
                        + "submitted_on_date, created_by, last_modified_by, created_on_utc, last_modified_on_utc) "
                        + "VALUES (?, ?, 1, false, ?, ?, ?, ?, ?, ?, 1, 1, NOW(), NOW())")) {
            ps.setLong(1, id);
            ps.setLong(2, loan);
            ps.setInt(3, type);
            ps.setObject(4, date);
            ps.setLong(5, amount);
            ps.setLong(6, amount * 8 / 10);
            ps.setLong(7, amount - amount * 8 / 10);
            ps.setObject(8, date);
            ps.executeUpdate();
        }
        db.txns.add(id);
    }

    private static void repayment(Db db, SplittableRandom rnd) throws SQLException {
        long loan = db.loans.get(rnd.nextInt(db.loans.size()));
        long amount = 500 + rnd.nextInt(5_000);
        insertTransaction(db, loan, 2, amount, LocalDate.of(2025, 6, 1).plusDays(rnd.nextInt(400)));
        try (PreparedStatement ps = db.conn.prepareStatement(
                "UPDATE m_loan SET principal_repaid_derived = principal_repaid_derived + ?, "
                        + "principal_outstanding_derived = GREATEST(principal_outstanding_derived - ?, 0), "
                        + "total_repayment_derived = total_repayment_derived + ?, "
                        + "total_outstanding_derived = GREATEST(total_outstanding_derived - ?, 0), "
                        + "last_modified_on_utc = NOW() WHERE id = ?")) {
            ps.setLong(1, amount * 8 / 10);
            ps.setLong(2, amount * 8 / 10);
            ps.setLong(3, amount);
            ps.setLong(4, amount);
            ps.setLong(5, loan);
            ps.executeUpdate();
        }
    }

    private static void updateLoan(Db db, SplittableRandom rnd) throws SQLException {
        long loan = db.loans.get(rnd.nextInt(db.loans.size()));
        try (PreparedStatement ps = db.conn.prepareStatement(
                "UPDATE m_loan SET is_npa = ?, loan_status_id = ?, last_modified_on_utc = NOW() WHERE id = ?")) {
            ps.setBoolean(1, rnd.nextInt(20) == 0);
            ps.setInt(2, rnd.nextInt(10) == 0 ? 600 : 300);
            ps.setLong(3, loan);
            ps.executeUpdate();
        }
    }

    private static void reverse(Db db, SplittableRandom rnd) throws SQLException {
        if (db.txns.isEmpty()) {
            return;
        }
        long txn = db.txns.get(rnd.nextInt(db.txns.size()));
        try (PreparedStatement ps = db.conn.prepareStatement(
                "UPDATE m_loan_transaction SET is_reversed = true, last_modified_on_utc = NOW() WHERE id = ?")) {
            ps.setLong(1, txn);
            ps.executeUpdate();
        }
    }

    private static void deleteTransaction(Db db, SplittableRandom rnd) throws SQLException {
        if (db.txns.size() < 10) {
            return;
        }
        int idx = rnd.nextInt(db.txns.size());
        long txn = db.txns.remove(idx);
        try (PreparedStatement ps = db.conn.prepareStatement("DELETE FROM m_loan_transaction WHERE id = ?")) {
            ps.setLong(1, txn);
            ps.executeUpdate();
        }
    }
}
