import pandas as pd 
import numpy as np 

def party_in_region(df_vote, df_poll):
    '''
    INPUT:
    Poll:
        Party   A   B
        Country pA pB
    Voting
        P/Reg   A   B
        Reg 1   r1A r1B
        .       .   .
        .       .   .
        .       .   .
        Reg N   rNA rNB
    CALCULATED: 
    Shares based on region:
        P/Reg   A   B
        Reg 1   G1A G1B
        .       .   .
        .       .   .
        .       .   .
        Reg N   GNA GNB

        Pni = rni / sum_j( rnj )
    OUTPUT:
    Party in reg:
        P/Reg   A   B
        Reg 1   g1A g1B
        .       .   .
        .       .   .
        .       .   .
        Reg N   gNA gNB

        gni = pi * Gni / sum_j( pj * Gnj )
    '''
    df_cal = df_vote.div(df_vote.sum(axis=1), axis=0).fillna(0)

    df_out = df_cal * df_poll.to_list()

    df_out = df_out.div(df_out.sum(axis=1), axis=0).fillna(0)

    return df_out


def region_in_party(df_vote, df_poll):
    '''
    INPUT:
    Poll:
        Party   A   B
        Country pA pB
    Voting
        P/Reg   A   B
        Reg 1   r1A r1B
        .       .   .
        .       .   .
        .       .   .
        Reg N   rNA rNB
    CALCULATED: 
    Shares based on region:
        P/Reg   A   B
        Reg 1   U1A U1B
        .       .   .
        .       .   .
        .       .   .
        Reg N   UNA UNB

        Uni = rni / sum_m( rmi )
    OUTPUT:
    Party in reg:
        P/Reg   A   B
        Reg 1   u1A u1B
        .       .   .
        .       .   .
        .       .   .
        Reg N   uNA uNB

        uni = pi * Ui / sum_j( pj * Unj )
    '''
    df_cal = df_vote.div(df_vote.sum(axis=0), axis=1).fillna(0)

    df_out = df_cal * df_poll.to_list()

    df_out = df_out.div(df_out.sum(axis=1), axis=0).fillna(0)

    return df_out