from cvs import scrape_fb,buildCommentsCSVs, buildPostCSVs


#Dictionary of the Medios
dicMedios = {'Nacion'	:'115872105050',
			'CRHoy'		:'265769886798719',
			'Financiero':'47921680333',
			'Semanario'	:'119189668150973',
			'Tico Times':'124823954224180',
			'Extra'		:'109396282421232'}

#MediaTic's App's Info
AppID = '264737207353432'
AppSecret = '460c5a58dd6ddd6997b2645b1ad37cdd'



#Nacion_posts = scrape_fb(AppID, AppSecret, dicMedios['Nacion'], "sample_posts.csv", '2.7', 'posts', '2017-10-02')	# Request post from La Nacion's FB page


#Comments = scrape_fb(AppID, AppSecret, '115872105050_10159475804280051', "sample_comments.csv", '2.7', 'comments', '2017-10-02')
#comments = scrape_fb('264737207353432','460c5a58dd6ddd6997b2645b1ad37cdd','115872105050_10159350223445051',scrape_mode="comments")

buildCommentsCSVs(AppID, AppSecret, dicMedios['Nacion'], 'nodos_comments_Nacion.csv', 'aristas_comments_Nacion.csv', version="2.10")


buildPostCSVs(AppID, AppSecret, dicMedios['Nacion'], 'nodos_posts_Nacion.csv', 'aristas_posts_Nacion.csv', version="2.10")


buildCommentsCSVs(AppID, AppSecret, dicMedios['CRHoy'], 'nodos_comments_CRHoy.csv', 'aristas_comments_CRHoy.csv', version="2.10")


buildPostCSVs(AppID, AppSecret, dicMedios['CRHoy'], 'nodos_posts_CRHoy.csv', 'aristas_posts_CRHoy.csv', version="2.10")

#KonzulTICAS
#115872105050/posts?fields=id,created_time,name,comments{id,message,comments{id,message}}&limit=10

#115872105050?fields=posts{id,created_time,name,comments{id,message,comments{id,message}}}&limit=10