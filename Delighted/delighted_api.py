import delighted
import pandas as pd
import sys


class Delighted:
    surveys_per_page = 100
    delighted_api_key = ''

    def __init__(self, delighted_api_key):
        self.delighted_api_key = delighted_api_key
        delighted.api_key = self.delighted_api_key

    def surveysToDict(self, survey_list):
        index = 1
        rows = dict()
        try:
            for survey in survey_list:
                row_filtered = dict()
                row_filtered['id'] = survey.id
                row_filtered['person'] = survey.person
                row_filtered['survey_type'] = survey.survey_type
                row_filtered['score'] = survey.score
                if survey.comment:
                    row_filtered['comment'] = survey.comment[:499]
                else:
                    row_filtered['comment'] = ''
                row_filtered['permalink'] = survey.permalink
                row_filtered['created_at'] = survey.created_at
                row_filtered['updated_at'] = survey.updated_at
                try:
                    row_filtered['source'] = survey.person_properties['Delighted Source']
                except:
                    row_filtered['source'] = ''
                try:
                    row_filtered['agent'] = survey.person_properties['agent']
                except:
                    row_filtered['agent'] = ''
                try:
                    row_filtered['company'] = survey.person_properties['company']
                except:
                    row_filtered['company'] = ''
                try:
                    row_filtered['is_multicart'] = survey.person_properties['is_multicart']
                except:
                    row_filtered['is_multicart'] = ''
                try:
                    row_filtered['order_id'] = survey.person_properties['order_id']
                except:
                    row_filtered['order_id'] = ''
                try:
                    row_filtered['service'] = survey.person_properties['service']
                except:
                    row_filtered['service'] = ''
                row_filtered['notes'] = survey.notes
                row_filtered['tags'] = survey.tags
                try:
                    row_filtered['additional_answers'] = survey.additional_answers
                except:
                    row_filtered['additional_answers'] = ''
                rows[index] = row_filtered
                index += 1
        except KeyError as e:
            pass
        return pd.DataFrame(rows).transpose()

    def surveysRequestToDict(self, surveys_request):
        index = 1
        rows = dict()
        try:
            for survey in surveys_request:
                print(survey)
                # rows[index] = row_filtered
                index += 1
        except KeyError as e:
            pass
        return pd.DataFrame(rows).transpose()

    def getSurveys(self):
        seguir = True
        pagina = 1
        delighted.api_key = self.delighted_api_key
        finalDataset = pd.DataFrame(dict())

        while seguir:
            try:
                surveys = delighted.SurveyResponse.all(per_page=self.surveys_per_page, page=pagina)
            except:
                pass
            if len(surveys) == 0:
                seguir = False
            else:
                dict_surveys = self.surveysToDict(surveys)
                finalDataset = finalDataset.append(dict_surveys)
                pagina = pagina + 1
        return finalDataset

    def getPeople(self):
        people = delighted.Person.list(per_page=100, auto_handle_rate_limits=True)
        index = 1
        delighted.api_key = self.delighted_api_key
        rows = dict()
        try:
            for person in people.auto_paging_iter():
                row_filtered = dict()
                row_filtered['id'] = person.id
                row_filtered['name'] = person.name
                row_filtered['email'] = person.email
                row_filtered['created_at'] = person.created_at
                row_filtered['last_sent_at'] = person.last_sent_at
                row_filtered['last_responded_at'] = person.last_responded_at
                row_filtered['next_survey_scheduled_at'] = person.next_survey_scheduled_at
                rows[index] = row_filtered
                index += 1
        except KeyError as e:
            pass
        except delighted.errors.GeneralAPIError as e:
            pass
        return pd.DataFrame(rows).transpose()
