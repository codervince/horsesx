##  The full models are as follows:

		class EventType(ModelBase):
		    __tablename__ = "hk_trackwork_type"
		    id = Column(Integer, primary_key=True)
		    Name = Column("name", String(100), unique=True)
		    UniqueConstraint('name', name='EventTypeName_uidx')


		class Owner(ModelBase):
		    __tablename__ = "owner"
		    __tableargs__ = ( CheckConstraint('Homecountry in ("HKG", "SIN", "AUS", "NZL", "RSA". "ENG", "IRE", "DUB", "IRE", "SCO", "MAC")'))
		    id = Column(Integer, primary_key=True)
		    Name = Column("name", String(255), unique=True)
		    Homecountry = Column('homecountry', String(3), nullable=False)
		    UniqueConstraint('name', name='OwnerName_uidx')


		 class Horse(ModelBase):
		    __tablename__ = "horse"
		    __tableargs__ = ( 
		    	CheckConstraint('Homecountry in ("HKG", "SIN", "AUS", "NZL", "RSA". "ENG", "IRE", "DUB", "IRE", "SCO", "MAC")'))
		    id = Column(Integer, primary_key=True)
		    Code = Column("code", String(6), nullable=False, unique=True)
		    Name = Column("name", String(255), nullable=False)
		    Sex = Column("sex", String(2), nullable=True)
		    Homecountry = Column('homecountry', String(3), nullable=False)
		    ImportType = Column("importtype", String(10), default="")
		    SireName = Column("sirename", String(255), default="")
		    DamName = Column("damname", String(255), default="")
		    DamSireName = Column("damsirename", String(255), default="")

		class HKTrackwork(ModelBase):
    		__tablename__ = "hk_trackwork"
    		__tableargs__ = ( 
        	UniqueConstraint('publicraceindex', name='HKTrackwork_PublicRaceIndex_uidx')
    		)
		    id = Column(Integer, primary_key=True)
		    EventDate = Column("eventdate", Date, nullable=False)
		    PublicRaceIndex = Column('publicraceindex', String, nullable=False, unique=True)
		    EventVenue = Column("eventvenue", String(100))
		    EventDescription = Column("eventdescription", String(255))
		    EventTypeid = Column("eventtypeid", Integer, ForeignKey('hk_trackwork_type.id'))
		    # Ownerid = Column("ownerid", Integer, ForeignKey("owner.id"))
		    Gearid = Column("gearid", Integer, ForeignKey("hk_gear.id"))
		    Horseid = Column("horseid", Integer, ForeignKey('horse.id'))

		class HKVet(ModelBase):
		    __tablename__ = "hk_vet"
		    __tableargs__ = ( 
		     UniqueConstraint('publicraceindex', name='HKVetPublicRaceIndex_uidx')
		    )

		    id = Column(Integer, primary_key=True)
		    Horseid = Column("horseid", Integer, ForeignKey('horse.id'))
		    PublicRaceIndex = Column('publicraceindex', String, nullable=False, unique=True)
		    EventDate = Column("eventdate", Date, nullable=False)
		    Details = Column("details", String(255))
		    PassedDate = Column("passeddate", Date, nullable=False)

		class Gear(ModelBase):
		    __tablename__ = "hk_gear"
		    id = Column(Integer, primary_key=True)
		    Name = Column("name", String(255), unique=True)
		    UniqueConstraint('name', name='GearName_uidx')

### Current usage - auto generated codes
python auto.py

## Next usage
python auto.py #scrapyd combine vet with horses2
### Previous usage, command line, one set of codes at once 
scrapy crawl horses2x -a horses=N250,P121,S054,P369,S011

