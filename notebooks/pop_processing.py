import pandas as pd
import numpy as np

def transform_house(x,columns):
        for col in columns:
            x[col] = x[col]/x['tot_acc']
        return x

def transform_pop(x,columns):
        for col in columns:
            x[col] = x[col]/x['total_pop']
        return x
class Loc:
    def __init__(self):
        self.data = pd.read_csv('../raw_data/loc_absolute.csv')

        self.house_col = ['elettricity', 'no_elettricity', 'water', 'no_water',
       'internet', 'no_internet', 'washer', 'microwave', 'computer',
       'payperview']
        self.pop_col = ['pob_fem', 'pob_male',
       'children_02', 'plus1_speaker', 'handicaps', 'no_handicaps',
       'no_school_18_24', 'analphabets_15', 'unemployed', 'no_health',
       'health', 'private_health', 'not_married', 'married', 'catholics',
       'protestants', 'other_religions', 'atheists']

    def ratio_houses(self):
        ratio_houses = self.drop_null()[['elettricity', 'no_elettricity', 'water', 'no_water',
       'internet', 'no_internet', 'washer', 'microwave', 'computer',
       'payperview','tot_acc']].astype('float64')
        return ratio_houses

    def drop_null(self):
        return self.data.apply(lambda x: x.replace('*', np.nan), axis=1).dropna()

    def new_df(self):
        self.ratio_houses = self.ratio_houses().apply(lambda x: transform_house(x,self.house_col), axis =1)
        self.ratio_houses['unique_number'] =  self.data['unique_number']
        return self.ratio_houses
    # def merge():
    #     bla= self.drop_null()



    def ratio_pop(self):
        ratio_pop = self.drop_null()[[ 'pob_fem', 'pob_male',
       'children_02', 'plus1_speaker', 'handicaps', 'no_handicaps',
       'no_school_18_24', 'analphabets_15', 'unemployed', 'no_health',
       'health', 'private_health', 'not_married', 'married', 'catholics',
       'protestants', 'other_religions', 'atheists','total_pop']].astype('float64')
        return ratio_pop


    def new_df2(self):
        self.ratio_pop = self.ratio_pop().apply(lambda x: transform_pop(x,self.pop_col), axis =1)
        self.ratio_pop['unique_number'] =  self.data['unique_number']
        self.ratio_pop['municipality']= self.data['key_municipality']
        return self.ratio_pop

    def merging_loc(self):
        self.final = self.new_df2().merge(self.new_df(), on = 'unique_number')
        self.final= self.final.drop(columns = ['total_pop', 'tot_acc'])
        return self.final[self.final['unique_number']!= 270040001]


class Ageb:
    def __init__(self):
        self.data = pd.read_csv('../raw_data/pob1.csv')


    def preprocessed(self):

        change_columns= {'AGEB_CONCAT':'unique_number',
                'ENTIDAD':'key_federal_state',
                'MUN': 'key_municipality',
                'AGEB': 'ageb',
                'LOC':'locality',
                'NOM_LOC': 'name_loc'}
        self.data = self.data[[              'ENTIDAD',
                                        'AGEB',
                                        'MUN',
                                        'LOC',
                                        'NOM_LOC'
                                        ]]

        self.data=self.data[self.data['MUN']==4]
        ageb=self.data[self.data['LOC']==1]
        ageb["AGEB_CONCAT"] = (ageb.ENTIDAD.astype(str).str.zfill(2) +\
                         ageb.MUN.astype(str).str.zfill(3) +\
                         ageb.LOC.astype(str).str.zfill(4) +\
                         ageb.AGEB.astype(str).str.zfill(4) \
                        )
        ageb.rename(columns=change_columns, inplace= True)
        return ageb

    def ratio_pop1(self):
        ratio = self.drop_null2()[['Unnamed: 0', 'ENTIDAD', 'NOM_ENT', 'MUN', 'NOM_MUN', 'LOC', 'NOM_LOC',
       'AGEB', 'MZA', 'pob_ttl', 'pob_fem', 'pob_mas', 'pob_3_5', 'pob_6_11',
       'pob_65', 'pob_indigena', 'pob_afro', 'pob_discap', 'pob_discap_mental',
       'pob_discap_motriz', '3_5_sinescuela', '6_11_sinescuela',
       '8_14_analphab', '15_nosecundaria', '18_24_analphab', 'pob_activa',
       'pob_desocupada', 'sin_salud', 'imss_salud', 'issste_salud',
       'secr_salud', 'priv_salud', 'catolica']].astype('float64')
        return ratio

    def drop_null2(self):
        return self.preprocessed().apply(lambda x: x.replace('*', np.nan), axis=1)\
                .apply(lambda x: x.replace('N/D', np.nan), axis=1).dropna()

    def new_df3(self):
        self.ratio_houses = self.ratio_houses2().apply(lambda x: transform_house(x,self.house_col), axis =1)
        self.ratio_houses['unique_number'] =  self.preprocessed()['unique_number']
        return self.ratio_houses


    def new_df4(self):
        self.ratio_pop = self.ratio_pop2().apply(lambda x: transform_pop(x,self.pop_col), axis =1)
        self.ratio_pop['unique_number'] =  self.preprocessed()['unique_number']
        return self.ratio_pop

    def merging_ageb(self):
        self.final = self.new_df3().merge(self.new_df4(), on = 'unique_number')
        return self.final.drop(columns = ['total_pop', 'tot_acc'])




