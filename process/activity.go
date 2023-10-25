package process

import (
	"datastream/logs"
	"datastream/types"
	"fmt"
	"math/rand"
	mathRand "math/rand"
	"strconv"
	"strings"
	"time"
)

var (
	i                                           int
	activityString                              string
	activityDateX                               time.Time
	activityDate1, activityDate2, activityDate3 time.Time
	activityDate                                time.Time
	flag                                        int
)

func GenerateActivityDate() {
	activityDate1 = activityDate.AddDate(0, 0, 1)
	activityDate2 = activityDate1.AddDate(0, 0, 2)
	activityDate3 = activityDate2.AddDate(0, 0, 3)
}

func GenerateActivity(p_id string) {
	percent := mathRand.Intn(101)
	addActivity := func(activityType int, date time.Time) {
		activityString += fmt.Sprintf("(%s, %d, %d, \"%s\"),", p_id, i, activityType, date.Format("2006-01-02"))
	}

	if percent <= 80 {
		if percent <= 30 {
			addActivity(1, activityDate)
			activityDate = activityDate1
			addActivity(3, activityDate)
		} else if percent <= 60 {
			addActivity(1, activityDate)
			activityDate = activityDate1
			addActivity(3, activityDate)
			activityDate = activityDate2
			addActivity(4, activityDate)
		} else {
			addActivity(1, activityDate)
			activityDate = activityDate1
			addActivity(3, activityDate)
			activityDate = activityDate2
			addActivity(4, activityDate)
			activityDate = activityDate3
			addActivity(7, activityDate)
		}
	} else if percent <= 90 {
		if percent <= 82 {
			addActivity(1, activityDate)
			activityDate = activityDate1
			addActivity(3, activityDate)
			activityDate = activityDate2
			addActivity(3, activityDate)
		} else if percent <= 84 {
			addActivity(1, activityDate)
			activityDate = activityDate1
			addActivity(3, activityDate)
			activityDate = activityDate2
			addActivity(3, activityDate)
			activityDate = activityDate3
			addActivity(4, activityDate)
		} else if percent <= 86 {
			addActivity(1, activityDate)
			activityDate = activityDate1
			addActivity(3, activityDate)
			activityDate = activityDate2
			addActivity(4, activityDate)
			activityDate = activityDate3
			addActivity(3, activityDate)
		} else if percent <= 88 {
			addActivity(1, activityDate)
			activityDate = activityDate1
			addActivity(3, activityDate)
			addActivity(4, activityDate)
			activityDate = activityDate2
			addActivity(3, activityDate)
			activityDate = activityDate3
			addActivity(4, activityDate)
		} else if percent <= 89 {
			addActivity(1, activityDate)
			addActivity(3, activityDate)
			activityDate = activityDate1
			addActivity(4, activityDate)
			activityDate = activityDate2
			addActivity(7, activityDate)
			activityDate = activityDate3
			addActivity(3, activityDate)
		} else {
			addActivity(1, activityDate)
			addActivity(3, activityDate)
			addActivity(4, activityDate)
			addActivity(7, activityDate)
			activityDate = activityDate2
			addActivity(3, activityDate)
			activityDate = activityDate3
			addActivity(4, activityDate)
		}
	} else {
		percent := rand.Intn(1001)
		if percent <= 960 {
			addActivity(1, activityDate)
		} else {
			flag = 0
			if percent <= 970 {
				activityDate = activityDate1
				addActivity(3, activityDate)
				activityDate = activityDate2
				addActivity(4, activityDate)
				activityDate = activityDate3
				addActivity(5, activityDate)
			} else if percent <= 980 {
				activityDate = activityDate1
				addActivity(3, activityDate)
				activityDate = activityDate2
				addActivity(4, activityDate)
				activityDate = activityDate3
				addActivity(6, activityDate)
			} else if percent <= 990 {
				activityDate = activityDate1
				addActivity(3, activityDate)
				activityDate = activityDate2
				addActivity(4, activityDate)
				activityDate = activityDate3
				addActivity(5, activityDate)
				addActivity(6, activityDate)
			} else {
				addActivity(2, activityDate)
			}
		}
	}
}

func ControlGenerateActivityFunction(ID string) {
	i++
	if i%10 == 0 {
		activityDateX = activityDateX.AddDate(0, 1, 0)
		activityDate = activityDateX
		GenerateActivityDate()
	} else {
		activityDate = activityDateX
	}
	GenerateActivity(ID)
	if i == 100 || flag == 0 {
		activityString = activityString[:len(activityString)-1]
	} else {
		ControlGenerateActivityFunction(ID)
	}
}
func ReturnContactsAndActivitiesStructs(contactsData types.Contacts) (types.ContactStatus, []types.ContactActivity,
	error) {
	activityDateX, _ = time.Parse("2006-01-02", "2023-01-01")
	activityDate = activityDateX
	GenerateActivityDate()
	i = 0
	flag = 1
	activityString = ""
	ControlGenerateActivityFunction(contactsData.ID)
	contact := types.Contacts{
		ID:      contactsData.ID,
		Name:    contactsData.Name,
		Email:   contactsData.Email,
		Details: contactsData.Details,
	}
	StatusContact := types.ContactStatus{
		Status:  flag,
		Contact: contact,
	}
	resultSeparateCh := make(chan []types.ContactActivity, 4)
	go RunSeparateContactActivities(activityString, 4, resultSeparateCh)
	MultiActivities := <-resultSeparateCh
	close(resultSeparateCh)
	return StatusContact, MultiActivities, nil
}

func RunSeparateContactActivities(activityString string, numColumns int, resultCh chan []types.ContactActivity) {
	multiActivities, err := SeparateContactActivities(activityString, numColumns)
	if err != nil {
		logs.NewLog.Error("Error Separating Contacts")
	}
	resultCh <- multiActivities
}

func SeparateContactActivities(activityString string, numColumns int) ([]types.ContactActivity, error) {
	activityStrings := strings.Split(activityString, "),(")
	var multiactivities []types.ContactActivity
	for _, activityStr := range activityStrings {
		activityStr = strings.Trim(activityStr, "()")
		parts := strings.Split(activityStr, ", ")
		if len(parts) >= numColumns {
			campaignID, err := strconv.Atoi(parts[1])
			if err != nil {
				//logs.NewLog.Error(fmt.Sprintf("Error converting CampaignID: %v", err))
				return nil, err
			}
			activityType, err := strconv.Atoi(parts[2])
			if err != nil {
				//	logs.NewLog.Error(fmt.Sprintf("Error converting ActivityType: %v", err))
				return nil, err
			}
			activitydateStr := parts[3]
			fmt.Println(activitydateStr)
			activitydateStr = strings.Trim(activitydateStr, `"`)
			layout := "2006-01-02"
			activitydate, _ := time.Parse(layout, activitydateStr)
			fmt.Println(activitydate)
			activity := types.ContactActivity{
				Contactid:    parts[0],
				Campaignid:   campaignID,
				Activitytype: activityType,
				Activitydate: activitydate,
			}
			multiactivities = append(multiactivities, activity)
		}
	}

	return multiactivities, nil
}
