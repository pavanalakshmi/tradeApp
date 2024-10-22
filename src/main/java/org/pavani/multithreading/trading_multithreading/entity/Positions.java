package org.pavani.multithreading.trading_multithreading.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.Getter;


@Entity
@Table(name = "positions")
@Data
public class Positions {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "position_id")
    private int positionId;

    @Getter
    @Column(name = "account_number")
    private String accountNumber;

    @Column(name = "CUSIP")
    private String cusip;

    @Column(name = "position")
    private int position;

    @Column(name = "version")
    private int version;

    public Positions() {
    }

}